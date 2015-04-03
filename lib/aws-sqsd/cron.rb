require 'aws-sdk-core'

require 'aws-sqsd/exception'
require 'aws-sqsd/logger'
require 'aws-sqsd/aws-sdk-perf-patch'
require 'aws-sqsd/dynamodb_utils'
require 'aws-sqsd/schedule_parser'
require 'aws-sqsd/sqsd_utils'
require 'aws-sqsd/bridge_logger'

require_relative '../../vendor/AWSMACLE/lib/leader_election'

class AWS::EB::SQSD::Cron
    include AWS::EB::SQSD::Logger
    include AWS::EB::SQSD::SQSDUtilities

    DispatchJob = Struct.new(:name, :next, :last, :job)

    DAEMON_ITERATION_PAUSE_TIME = 10
    LEADER_ITERATION_PAUSE_TIME = 5
    DDB_THROTTLE_BACKOFF_TIME = 0.25

    def initialize(config:, queue:)
        @config = config
        @queue = queue

        # load job schedule and compute pending time
        @scheduled_jobs = AWS::EB::SQSD::ScheduleParser::load_schedule
        if ! @scheduled_jobs.empty?
            leader_election_daemon  # initialize leader daemon
            @job_pending_times = init_job_pending_times # initialize schedule time
        end
    end

    def start
        return false if @scheduled_jobs.empty?
        leader_election_daemon.start
        sleep 0.1 until leader_election_daemon.status == :running
        @thread = Thread.new do
            log 'scheduler', 'Starting scheduler'
            loop do
                run
                sleep DAEMON_ITERATION_PAUSE_TIME
            end
        end
    end

    private
    def run
        begin
            if leader_election_daemon.leader?
                scheduled_dispatch_times.each do |job|
                    hole_detection job
                end

                while leader_election_daemon.leader?
                    update_scheduled_jobs
                    pending_time = @job_pending_times.values.min.to_f
                    scheduled_jobs_to_run = scheduled_dispatch_times(pending_time)
                    verbose_log 'scheduler', "next message gets sent in #{pending_time} seconds"
                    debug_log 'scheduler', "job pending time: #{@job_pending_times}"
                    sleep pending_time
                    send_messages scheduled_jobs_to_run
                    sleep LEADER_ITERATION_PAUSE_TIME
                end
            end

        rescue Exception => e
            debug_log 'scheduler', AWS::EB::SQSD.format_exception(e, stack_trace: true)
            log 'scheduler', %[fatal error, because: #{e.class}: #{e.message}]
            leader_election_daemon.demote
            raise e
        end
    end

    private
    def send_message(dispatch_job)
        begin
            debug_log 'scheduler', "sending message to queue for job '#{dispatch_job.name}'"
            @queue.send_message 'elasticbeanstalk scheduled job', 
                :message_attributes => { 'beanstalk.sqsd.path' => { :data_type => 'String', :string_value => dispatch_job.job.path },
                                         'beanstalk.sqsd.task_name' => { :data_type => 'String', :string_value => dispatch_job.name },
                                         'beanstalk.sqsd.scheduled_time' => { :data_type => 'String', :string_value =>  dispatch_job.next.to_s } }

            record_message dispatch_job
        rescue Exception => e
            # failed to send or record a message, give up leader
            log 'scheduler', "dropping leader, due to failed to send message for job '#{dispatch_job.name}', because: #{AWS::EB::SQSD.format_exception(e)}"
            leader_election_daemon.demote
        end
    end

    private
    def send_messages(scheduled_jobs_to_run)
        # send messages asynchronously
        defer(log_category: 'scheduler') do
            scheduled_jobs_to_run.each do |dispatch_job|
                send_message dispatch_job
            end
        end
    end

    # Update database to record last message sent time for a job
    # Do best-effort recording but swallow error if still fail after retries
    private
    def record_message(dispatch_job)
        schedule_time = dispatch_job.next.to_f
        retries = @config.record_message_retries

        begin
            debug_log 'scheduler', "recording time of last sent message for job '#{dispatch_job.name}'"
            write_to_table id: dispatch_job.name, update_time: Time.now.utc.to_f, schedule_time: schedule_time
        rescue Aws::DynamoDB::Errors::ProvisionedThroughputExceededException
            log 'scheduler', %[sent message for job '#{dispatch_job.name}' is not recorded to database: DynamoDB table #{@config.registry_table} exceeded throughput capacity] if retries == 0
            sleep DDB_THROTTLE_BACKOFF_TIME
            retry if (retries -= 1) >= 0
        rescue => e
            log 'scheduler', %[sent message job '#{dispatch_job.name}' is not recorded to database, because: #{e.class}: #{e.message}] if retries == 0
            retry if (retries -= 1) >= 0
        end
    end

    private
    def hole_detection(dispatch_job)
        now = Time.now.utc.to_f
        retries = @config.hole_detection_retries
        hole_detected = false
        last_value_logged = nil
        begin
            last_value_logged = latest_record(id: dispatch_job.name)['schedule_time'].to_f
            debug_log 'scheduler', "hole detection for '#{dispatch_job.name}' last value #{last_value_logged} local value #{dispatch_job.last.to_f}"
            conditional_write_to_table id: dispatch_job.name, update_time: now, schedule_time: dispatch_job.last.to_f
            debug_log 'scheduler', "verified job '#{dispatch_job.name}' was executed at last scheduled time"
        rescue Aws::DynamoDB::Errors::ConditionalCheckFailedException
            hole_detected = true    # database record is older than last scheduled time
        rescue Aws::DynamoDB::Errors::ProvisionedThroughputExceededException
            log 'scheduler', %[DynamoDB table #{@config.registry_table} exceeded throughput capacity] if retries == 0
            sleep DDB_THROTTLE_BACKOFF_TIME
            retry if (retries -= 1) >= 0
            return  # give up writing to database
        rescue => e
            log 'scheduler', %[message not sent, #{e.class}: #{e.message}] if retries == 0
            retry if (retries -= 1) >= 0
            raise
        end

        if hole_detected && now - last_value_logged < @config.message_resend_grace_period
            log 'scheduler', "Sending missed message for '#{dispatch_job.name}'"
            send_message(dispatch_job)
        end
    end

    private
    def write_to_table(id:, update_time:, schedule_time:)
        dynamo_db.update_item table_name: @config.registry_table,
                              key: { 'id' => id },
                              expression_attribute_values: {
                                  ":utime" => update_time,
                                  ":stime" => schedule_time,
                              },
                              update_expression: "SET update_time = :utime, schedule_time = :stime"

    end

    private
    def conditional_write_to_table(id:, update_time:, schedule_time:)
        dynamo_db.update_item(table_name: @config.registry_table,
                              key: { 'id' => id },
                              expression_attribute_values: {
                                  ":utime" => update_time,
                                  ":stime" => schedule_time,
                              },
                              update_expression: "SET update_time = :utime, schedule_time = :stime",
                              condition_expression: "schedule_time >= :stime")
    end


    private
    def latest_record(id:)
        r = dynamo_db.get_item(table_name: @config.registry_table,
                               key: { 'id' => id },
                               consistent_read: true)
        r.item.to_h
    end

    private
    def update_scheduled_jobs
        now = Time.now.utc
        @scheduled_jobs.each do |name, job|
            next_time = job.parser.next(now)
            @job_pending_times[name] = (utc_time(next_time) - now).floor
        end
    end

    private
    def scheduled_dispatch_times(time_to_next = nil)
        now = Time.now.utc
        dispatch_jobs = []
        @scheduled_jobs.each do |name, job|
            if time_to_next.nil? or @job_pending_times[name] == time_to_next
            next_time    = job.parser.next(now)
            last_time    = job.parser.last(now)
            dispatch_job = DispatchJob.new(name,
                                           utc_time(next_time),
                                           utc_time(last_time),
                                           job)
            dispatch_jobs << dispatch_job
            end
        end
        dispatch_jobs
    end

    private
    def init_job_pending_times
        job_pending_times = {}
        now = Time.now.utc
        @scheduled_jobs.each do |name, job|
            next_time = job.parser.next(now)
            job_pending_times[name] = (utc_time(next_time)- now).floor
        end

        debug_log 'scheduler', "job pending times: #{job_pending_times}"
        log 'scheduler', "initialized #{job_pending_times.length} job's pending time"
        job_pending_times
    end

    private
    def utc_time(time)
        (time + time.utc_offset).utc
    end

    private
    def dynamo_db
        @dynamo_db ||= Aws::DynamoDB::Client.new(:region             => @config.region,
                                                 :simple_attributes  => true,
                                                 :retry_limit        => 5)
    end

    private
    def leader_election_daemon
        @leader_election_daemon ||= AWS::EB::LeaderElection.create :workercount => true,
                         :table_name => @config.registry_table, :region => @config.region,
                         :logger => AWS::EB::SQSD::BridgeLogger.new
    end
end
