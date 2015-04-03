require 'aws-sdk'
require 'benchmark'

require 'leader_election/scheduler'
require 'leader_election/config'
require 'leader_election/storage_manager'
require 'leader_election/version'
require 'leader_election/logger'

module AWS
    module EB
        module LeaderElection
            class Daemon
                include StorageManager
                include LoggerWriter

                attr_reader :config

                def initialize(options={})
                    Thread.abort_on_exception = true

                    @config = Config.new options
                    @heartbeat_scheduler = Scheduler.new
                    @workercount_scheduler = Scheduler.new if config.workercount

                    if config.logger
                        @logger = config.logger
                    else
                        @logger = Logger.new '/dev/null'
                        @logger.level = Logger::FATAL
                    end

                    @last_expiration_time = Time.now
                    @last_count_time = nil
                    @leader = false

                    verify_table
                    info "initialized leader election"
                end

                # return whether the daemon is currently leader
                def leader?
                    if status != :running
                        false
                    elsif @leader && ((@last_expiration_time - Time.now) < config.tolerant_clock_difference)
                        warn "failed to refresh leadership. leadership expired at: #{@last_expiration_time.utc.iso8601}"
                        info "current role: worker"
                        giveup_leadership
                    else
                        @leader
                    end
                end

                # stop the daemon
                def stop
                    unless status == :stopped
                        @heartbeat_scheduler.stop
                        @workercount_scheduler.stop if config.workercount

                        info "stopping leader election"
                        giveup_leadership
                    end
                    nil
                end

                def status
                    if @heartbeat_scheduler.status == :starting || (config.workercount && @workercount_scheduler.status == :starting)
                        :starting
                    elsif @heartbeat_scheduler.status == :running && (!config.workercount || @workercount_scheduler.status == :running)
                        :running
                    elsif @heartbeat_scheduler.status == :stopped && (!config.workercount || @workercount_scheduler.status == :stopped)
                        :stopped
                    else
                        :stopping
                    end
                end

                # start the daemon, start to update or check the DynamoDB table
                def start
                    unless status == :stopped
                        raise LeaderElection::Errors::DaemonAlreadyRunningException
                    end

                    info "Starting leader election"
                    info "current role: worker"

                    # start leader heart beat thread
                    @heartbeat_scheduler.start do
                        if leader?
                            time_spent = Benchmark.realtime do
                                leader_update
                            end
                            if time_spent < config.leader_update_interval
                                sleep_time = (config.leader_update_interval - time_spent)
                                debug "sleep #{sleep_time} ..."
                                sleep sleep_time
                            end
                        else
                            time_spent = Benchmark.realtime do
                                worker_check
                            end

                            if !leader? && time_spent < config.worker_check_interval
                                sleep_time = (config.worker_check_interval - time_spent)
                                debug "sleep #{sleep_time} ..."
                                sleep sleep_time
                            end

                        end           
                    end

                    # start worker count thread
                    if config.workercount
                        @workercount_scheduler.start do
                            if leader?
                                time_spent = Benchmark.realtime do 
                                    @worker_num = worker_count
                                end
                                if @worker_num
                                    debug "number of workers: " + @worker_num.to_i.to_s
                                else
                                    debug "can not get worker count"
                                end
                                sleep (config.worker_count_interval - time_spent) if time_spent < config.worker_count_interval
                            else
                                time_spent = Benchmark.realtime do
                                    register_as_worker
                                end

                                sleep (config.worker_registration_interval - time_spent) if time_spent < config.worker_registration_interval
                            end
                        end
                    end

                    nil
                end

                # demote ourselves from a leader role
                def demote
                    if leader?
                        info "demote requested. giving up leadership. current role: worker"
                        giveup_leadership

                        @heartbeat_scheduler.rest :time => config.rest_time
                        true
                    else
                        false
                    end
                end

            end
        end
    end
end
