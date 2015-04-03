require 'leader_election/exceptions'

module AWS
    module EB
        module LeaderElection
            class Scheduler
                def initialize
                    @scheduled_exit = false
                    @stepdown_wait = 0
                    @status = :stopped
                end

                def start
                    @scheduled_exit = false
                    unless @status == :stopped
                        raise LeaderElection::Errors::SchedulerAlreadyRunningException
                    end

                    @status = :starting
                    @running_thread = Thread.new do
                        until @scheduled_exit
                            if @stepdown_wait != 0
                                sleep @stepdown_wait
                                @stepdown_wait = 0
                            end

                            yield
                        end
                        @status = :stopped
                    end
                    @status = :running
                end

                #rest for a period of time after giving up leadership
                def rest(time:)
                    @stepdown_wait = time
                end

                #return whether the scheduler is running
                def status
                    if @status == :running && !@running_thread.alive?
                        @status = :stopped
                    end
                    @status
                end

                #stop the scheduler
                def stop
                    if @status == :stopped
                        raise LeaderElection::Errors::SchedulerAlreadyStoppedException
                    end
                    @scheduled_exit = true
                    @status = :stopping
                end
            end
        end
    end
end
