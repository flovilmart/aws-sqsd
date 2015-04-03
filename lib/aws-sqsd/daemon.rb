require 'aws-sdk'
require 'em-http-request'

require 'aws-sqsd/config'
require 'aws-sqsd/version'
require 'aws-sqsd/logger'
require 'aws-sqsd/counters'
require 'aws-sqsd/dynamodb_dedup'
require 'aws-sqsd/aws-sdk-perf-patch'
require 'aws-sqsd/cron'
require 'aws-sqsd/sqsd_utils'

class AWS::EB::SQSD::Daemon
    include AWS::EB::SQSD::Version
    include AWS::EB::SQSD::Logger
    include AWS::EB::SQSD::DynamoDBDedup
    include AWS::EB::SQSD::SQSDUtilities

    attr_reader :config, :http_connection_pool, :counters


    Thread.abort_on_exception = true

    def initialize(options={})
        log 'init', "initializing aws-sqsd #{full_version}"
        @config = AWS::EB::SQSD::Config.new options

        $debug = true if @config.debug
        $verbose = true if @config.verbose

        @http_url = %[#{config.http_url}:#{config.http_port}#{config.http_path}]
        @max_message_backlog = config.backlog_size ? config.backlog_size : config.concurrent_sqs_polls * config.sqs_max_batch_size
        @sqs_wait_time_seconds = config.sqs_max_wait_time_seconds

        sqs = AWS::SQS.new :region => config.region,
                           :use_ssl => config.sqs_ssl,
                           :sqs_verify_checksums => config.sqs_verify_checksums
        @queue = sqs.queues[config.queue_url]

        @counters = AWS::EB::SQSD::Counters.new
        @message_process_times = []
        @successful_sqs_poll = false

        @scheduler = AWS::EB::SQSD::Cron.new config: @config, queue: @queue

        # Include optional monitor. The module has to implement monitor() which is called from main event loop
        # Give monitor chance to initialize itself if needed
        extend config.monitor if config.monitor
    rescue *options[:fatal_exceptions] => e
        log 'init', AWS::EB::SQSD.format_exception(e, stack_trace: false)
        raise
    rescue => e
        log 'init', AWS::EB::SQSD.format_exception(e, stack_trace: true)
        raise
    end


    def enact
        Thread.new { EventMachine.run }
        sleep 0.1 until EventMachine.reactor_running?

        EventMachine.threadpool_size = config.threads
        EventMachine.kqueue if EventMachine.kqueue?
        EventMachine.epoll  if EventMachine.epoll?

        initialize_connection_pool
        initialize_batch_delete
        poll_timer = initialize_poll_timer
        initialize_trap poll_timer
        @scheduler.start

        # start the monitor if the method exists?
        monitor if respond_to? :monitor

        log 'start', "polling #{@queue.url}"
        sleep 5 while EventMachine.reactor_running?
    end

    private
    def poll_messages(max_limit:)
        defer(log_category: 'pollers') do
            log 'sqs', 'polling...' if config.debug

            messages = []
            messages_count = 0

            poll_limit = [max_limit, 10].compact.min
            log 'pollers', "polling #{poll_limit} messages" if config.debug
            sqs_receive_options = {
                :limit => poll_limit, 
                :wait_time_seconds => @sqs_wait_time_seconds,
                :visibility_timeout => config.visibility_timeout,
                :attributes => [:receive_count, :first_received_at, :sent_at, :sender_id],
                :message_attribute_names => ['*']
            }
            
            try(log_category: 'pollers', :retries => 0, :on_error => []) {
                messages = (poll_limit == 1) ? [(@queue.receive_message sqs_receive_options)].compact : (@queue.receive_message sqs_receive_options)
                messages_count = messages.count
                counters.increase :messages_received, messages_count    

                @last_messages_received_at = Time.now if messages_count > 0
                @successful_sqs_poll = true
            }
            counters.decrease :concurrent_sqs_queries

            start_time = Time.now
            log 'sqs', %[#{messages_count} new message(s)] if config.verbose && messages_count > 0
    
            messages.each do |msg|
                # deferring each message lowers throughput, hence we don't
                if Time.now > msg.first_received_at + config.retention_period
                    counters.increase :message_count
                    since_first_seen = (Time.now - msg.first_received_at).to_i
                    log 'expired-msg', %[#{msg.id} was first received #{since_first_seen} seconds ago. deleting], start_time
                    delete_message msg, start_time
                elsif config.dedup && ! process_message?(msg, start_time)
                    counters.increase :message_count
                else
                    post_message msg, start_time
                end
            end
        end
    end

    private
    def post_message(msg, start_time)
        try(log_category: 'post', :retries => 0) do
            http_connection_pool.perform do |connection|
                log 'post', msg.id, start_time if config.verbose

                body = nil
                if config.via_sns
                    # AWS::SQS::ReceivedSNSMessage#body raises TypeError unless message is from SNS topic
                    begin body = msg.as_sns_message.body; rescue RuntimeError; end
                end
                body = msg.body unless body

                new_http_path = msg.message_attributes['beanstalk.sqsd.path']
                new_http_path = new_http_path[:string_value] if new_http_path
                task_name = msg.message_attributes['beanstalk.sqsd.task_name']
                task_name = task_name[:string_value] if task_name
                dispatch_job = msg.message_attributes['beanstalk.sqsd.scheduled_time']
                dispatch_job = Time.parse(dispatch_job[:string_value]).utc.iso8601 if dispatch_job

                debug_log 'message', "Message has new path: #{new_http_path}"

                head = {
                    'content-type'                 => config.mime_type,
                    'User-Agent'                   => "aws-sqsd/#{version}",
                    'X-aws-sqsd-msgid'             => msg.id,
                    'X-aws-sqsd-receive-count'     => msg.receive_count,
                    'X-aws-sqsd-first-received-at' => msg.first_received_at.utc.iso8601,
                    'X-aws-sqsd-sent-at'           => msg.sent_at.utc.iso8601,
                    'X-aws-sqsd-queue'             => File.basename(msg.queue.url),   # queue name
                    'X-aws-sqsd-path'              => new_http_path,
                    'X-aws-sqsd-sender-id'         => msg.sender_id
                }

                msg_attr_keys = msg.message_attributes.reject do |key|
                    key[/^beanstalk.sqsd.*/]
                end  
                msg_attr_keys.each do |key, value|
                    if value[:data_type].start_with?('Number','String')
                        header_name = "X-aws-sqsd-attr-#{key}"
                        if head.has_key? header_name
                            log 'attribute skipped', "Message attribute #{key} omitted from HTTP header: duplicate key"
                        else
                            head[header_name] = value[:string_value]
                        end
                    else
                        log 'attribute skipped', "Message attribute #{key} omitted from HTTP header: unsupported data type #{value[:data_type]}"
                    end
                end

                counters.increase :concurrent_http_requests
                http = if dispatch_job
                    head['X-aws-sqsd-scheduled-at'] = dispatch_job
                    head['X-aws-sqsd-taskname'] = task_name
                    dispatch_post connection, new_http_path, nil, head
                else
                    post connection, body, head
                end
                log 'message', "sent to %[#{config.http_url}:#{config.http_port}#{new_http_path}]"

                message_processing_time = Time.now
                # Test avg message time
                log 'messages', "Current average message time is: #{avg_processing_time}" if config.debug

                http.callback do
                    counters.decrease :concurrent_http_requests
                    counters.increase :message_count

                    if http.response_header.status == 200
                        counters.increase :ok_count
                        log 'ok', %[#{msg.id} - #{http.response_header.status}], start_time if config.verbose

                        start_time = [start_time, Time.now]
                        message_processed msg, start_time if config.dedup
                        delete_message msg, start_time

                        # Only record message process time if it succeeds
                        message_processing_time = Time.now - message_processing_time
                        add_message_process_time(message_processing_time)
                    else
                        counters.increase :error_count
                        log 'http-err', %[#{msg.id} (#{msg.receive_count}) #{http.response_header.status}], start_time unless config.quiet
                        msg.visibility_timeout = config.error_visibility_timeout if config.error_visibility_timeout
                        message_failed msg, [start_time, Time.now] if config.dedup
                    end
                end
                http.errback do
                    # failed connection - e.g. DNS, timeout, connection refused
                    counters.decrease :concurrent_http_requests

                    if http.error == 'connection closed by server'
                        log 'conn-closed', %[#{msg.id} (#{msg.receive_count})], start_time if config.debug
                        post_message msg, start_time
                    else
                        counters.increase :message_count
                        counters.increase :error_count
                        log 'socket-err', %[#{msg.id} (#{msg.receive_count}) #{http.error}], start_time unless config.quiet
                        msg.visibility_timeout = config.error_visibility_timeout if config.error_visibility_timeout
                        message_failed msg, [start_time, Time.now] if config.dedup
                    end
                end
            end
        end
    end
    private
    def post(connection, body, head)
        if config.keepalive
            connection.post :keepalive => true, :body => body, :head => head
        else
            EventMachine::HttpRequest.new(@http_url).post :body => body, :head => head
        end
    end

    private
    def create_http_connection
        debug_log 'http', %[created new connection]
        EventMachine::HttpRequest.new @http_url, :connect_timeout    => config.connect_timeout, 
                                                 :inactivity_timeout => config.inactivity_timeout
    end

    private
    def dispatch_post(connection, path, body, head)
        debug_log 'http', %[created new connection]
        connection.post :path => path, :body => body, :head => head
    end
    
    private
    def delete_message(msg, start_time=nil)
        if config.sqs_batch_delete
            @sqs_delete_queue << msg.handle

            delete_message_batch if @sqs_delete_queue.size >= config.sqs_batch_delete_size
        else
            defer(log_category: 'delete', :retries => 5) do
                msg.delete
                log 'delete', msg.id, start_time if config.verbose
            end
        end
    end

    private
    def delete_message_batch
        defer(log_category: 'delete') do
            start_time = Time.now
            begin
                message_batch = @sqs_delete_queue.shift(config.sqs_batch_delete_size)
                try(log_category: 'delete') { @queue.batch_delete message_batch } if message_batch.any?
            end
            log 'delete', %[#{message_batch.count} message(s). #{@sqs_delete_queue.count} left], start_time if config.verbose
        end
    end

    private
    def messages_in_queue
        counters[:messages_received] - counters[:message_count]
    end

    private
    def determine_sqs_batch_delete
        log 'autotuning', "sqs_batch_size is #{!(@max_message_backlog < config.sqs_batch_delete_size)}" if config.debug
        if @max_message_backlog < config.sqs_batch_delete_size
            config.sqs_batch_delete = false
        else
            config.sqs_batch_delete = true
        end
    end


    private
    def change_backlog_size

        @max_message_backlog = avg_processing_time < config.autotuning_threshold_slowest ? config.http_connections : 0
        log 'autotuning', "New maximum backlog size: #{@max_message_backlog}" if config.debug
    end

    private 
    def change_num_pollers
        sqs_limit = [config.current_sqs_max_batch_size, config.sqs_max_batch_size].min
        num_pollers = sqs_limit < config.sqs_max_batch_size ? 1 : [@max_message_backlog, config.http_connections].max / sqs_limit.to_f
        log 'autotuning', "number of pollers: #{num_pollers.ceil}" if config.debug
        @concurrent_sqs_polls = num_pollers.ceil
    end

    private
    def current_backlog_size
        http_connection_pool.num_waiting
    end

    private
    def enable_autotuning
        valid_message_processing_time = avg_processing_time > config.autotuning_threshold_slow
    end

    private
    def current_http_connections
        counters[:concurrent_http_requests]
    end

    private
    def poll_mode_switch_due?(time)
        unless @last_poll_mode_switch
            false 
        else
            (time - @last_poll_mode_switch) > config.poll_mode_switch_interval
        end
    end

    private
    def stop_autotuning
        @concurrent_sqs_polls = config.concurrent_sqs_polls
        config.current_sqs_max_batch_size = config.sqs_max_batch_size
        @max_message_backlog = config.backlog_size ? config.backlog_size : @concurrent_sqs_polls * config.sqs_max_batch_size
        determine_sqs_batch_delete if config.lock_sqs_batch_delete
        @autotuning_active = false
        log 'autotuning', "Deactivate autotuning with backlog of #{@max_message_backlog}" if config.verbose
    end

    private
    def start_autotuning
        change_backlog_size unless config.backlog_size
        change_num_pollers if config.lock_concurrent_sqs_polls
        determine_sqs_batch_delete if config.lock_sqs_batch_delete
        @autotuning_active = true
        log 'autotuning', "Turning on autotuning backlog size is #{@max_message_backlog}" if config.verbose
    end

    private
    def avg_processing_time
        if @message_process_times.empty?
            config.autotuning_threshold_slowest
        else
            @message_process_times.reduce(&:+) / @message_process_times.length
        end
    end

    private
    def add_message_process_time(time)
        @message_process_times.shift unless @message_process_times.length < 50
        @message_process_times.push(time)
    end

    private
    def initialize_connection_pool
        @http_connection_pool = EventMachine::Pool.new

        # on error create a new connection
        http_connection_pool.on_error { http_connection_pool.add(create_http_connection) }

        # populate http connection pool
        config.http_connections.times do
            http_connection_pool.add create_http_connection
        end
    end

    private
    def initialize_batch_delete
        # timer to ensure that the queue is empty if SQS batch delete is enabled
        if config.sqs_batch_delete
            @sqs_delete_queue = []

            EventMachine::PeriodicTimer.new(config.batch_delete_timer) do
                delete_message_batch if @sqs_delete_queue.size > 0
            end
        end
    end

    private
    def initialize_poll_timer
        @concurrent_sqs_polls = config.concurrent_sqs_polls
        @last_poll_mode_switch = Time.now
        @last_messages_received_at = Time.now - 60

        # Start the daemon as a slow application
        start_autotuning

        # start SQS poll timer
        EventMachine::PeriodicTimer.new(config.poll_timer) do
            message_delta = @max_message_backlog == 0 ? config.http_connections - current_http_connections : @max_message_backlog - current_backlog_size
            backlog_threshold = @autotuning_active ? 0.5 : 0.2
            config.current_sqs_max_batch_size = message_delta

            log 'pollers', "limit for sqs pollers: #{[config.current_sqs_max_batch_size, config.sqs_max_batch_size].min}" if config.debug
            log 'pollers', "number of pollers: #{@concurrent_sqs_polls}" if config.debug
            log 'pollers', "message_delta is: #{message_delta}" if config.debug
            #if message_delta is larger than zero then poll messages according to the message_delta otherwise so not poll messages
            ((message_delta > 0) ? [message_delta / config.sqs_max_batch_size, 1].max : 0).times do |i|
                if counters[:concurrent_sqs_queries] >= @concurrent_sqs_polls
                    log 'sqs', %[skipping cycle. #{counters[:concurrent_sqs_queries]} pending SQS polls] if config.debug
                    break
                # This logic will fill the backlog when it is half empty with slow applications, or when 20% has been depleted for fast applications
                elsif message_delta < @max_message_backlog * backlog_threshold
                    log 'sqs', %[skipping cycle. #{messages_in_queue} pending messages] if config.verbose
                    break
                else
                    counters.increase :concurrent_sqs_queries
                    poll_messages :max_limit => config.current_sqs_max_batch_size
                end
            end

            # optimize the amount of concurrent SQS polls
            #
            now = Time.now
            if poll_mode_switch_due?(now) && now - @last_messages_received_at > config.idle_mode_delay && @concurrent_sqs_polls != 1
                log 'sqs', %[switching to one SQS poll thread] if config.verbose
                @concurrent_sqs_polls = 1
                @sqs_wait_time_seconds = config.sqs_max_wait_time_seconds
                @last_poll_mode_switch = now
            elsif poll_mode_switch_due?(now) && now - @last_messages_received_at < config.idle_mode_delay && @concurrent_sqs_polls == 1 && @max_message_backlog > 10
                log 'sqs', %[switching to #{config.concurrent_sqs_polls} SQS poll threads] if config.verbose
                @concurrent_sqs_polls = config.concurrent_sqs_polls
                @sqs_wait_time_seconds = config.sqs_wait_time_seconds
                @last_poll_mode_switch = now
            elsif poll_mode_switch_due?(now) && enable_autotuning
                # change the backlog size and pollers if no backlog size was given
                start_autotuning
                @last_poll_mode_switch = now
            elsif poll_mode_switch_due?(now) && !enable_autotuning && @autotuning_active
                stop_autotuning
                @last_poll_mode_switch = now
            end
        end
    end

    private
    def initialize_trap(poll_timer)
        # stop the SQS poll timer on signal and wait for the messages to be processed
        trap("SIGINT") do
            sleep 1 until EventMachine.reactor_running?
            poll_timer.cancel

            # skip draining if there are no messages in the local queue
            if messages_in_queue > 0
                log 'drain', "draining the internal queue..."

                drained = false
                config.drain_timeout.times do
                    log 'drain', "#{messages_in_queue} messages left" if config.verbose
                    if EventMachine.defers_finished?
                        drained = true
                        break
                    end
                    sleep 1
                end
                log 'timeout', 'draining timed out' unless drained
            end

            log 'stop', 'stopping the daemon'
            EventMachine.stop
        end if config.trap
    end

    private
    def region
        config.region
    end

end
