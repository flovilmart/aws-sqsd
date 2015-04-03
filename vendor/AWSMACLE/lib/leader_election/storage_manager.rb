require 'aws-sdk-core'
require 'leader_election/exceptions'
require 'open-uri'
require 'timeout'
require 'socket'

module AWS
    module EB
        module LeaderElection
            module StorageManager
                TIMEOUT = 60   # 1 minute
                LEADER_ELECTION_RECORD_NAME = "leader-election-record"
                REGISTRATION_RECORD_NAME = "registration-record"
                DELAY_ON_TABLE_DROP = 10

                # verify that the table exists and current role has all required permissions
                private
                def verify_table
                    update_registration
                    read_registration
                end

                # initialize heartbeat record in DynamoDB
                private
                def init_heartbeat_record
                    dynamo_db.put_item :table_name => config.table_name,
                        :item  => {
                        :id               => LEADER_ELECTION_RECORD_NAME, 
                        :leader_timestamp => -1, 
                        :leader_id        => 0
                    },
                    :expected => {
                        'id' => {
                            :comparison_operator => "NULL"
                        },
                        'leader_id' => {
                            :comparison_operator => "NULL"
                        }
                    },
                    :conditional_operator => 'OR'
                rescue Aws::DynamoDB::Errors::ConditionalCheckFailedException
                    debug "record already exists"
                end

                # reset timestamp of dynamoDB to claim that this instance is no longer leader
                private
                def reset_timestamp
                    dynamo_db.update_item :table_name => config.table_name,
                        :key => {
                        'id' => LEADER_ELECTION_RECORD_NAME
                    },
                    :attribute_updates => {
                        'leader_timestamp' => {
                            :value  => -1,
                            :action => 'PUT'
                        }
                    },
                    :expected => {
                        'leader_id' => {
                            :attribute_value_list => [
                                daemon_id
                            ],
                            :comparison_operator => 'EQ'
                        }
                    }
                rescue Aws::DynamoDB::Errors::ServiceError => e
                    warn "resetting leadership timestamp failed: (#{e.class}): #{e.message}"
                end

                # register to get an unique daemon id
                private
                def daemon_id
                    @daemon_id ||= begin
                                       worker_id = format "%10.10d", update_registration
                                       %[#{config.instance_id}.#{worker_id}]
                                   end
                end

                private
                def update_registration
                    item = dynamo_db.update_item :table_name => config.table_name,
                                                 :key => {
                                                     'id' => REGISTRATION_RECORD_NAME
                                                 },
                                                 :attribute_updates => {
                                                     'worker_id' => {
                                                         :value  => 1,
                                                         :action => 'ADD'
                                                     }
                                                 },
                                                 return_values: 'UPDATED_NEW'

                    item.attributes["worker_id"].to_i
                end

                # update the leadership record
                private
                def leader_update
                    debug 'update leader heart beat...'
                    leader_heartbeat
                rescue Aws::DynamoDB::Errors::ResourceNotFoundException
                    error "DynamoDB table '#{config.table_name}' not found. sleeping..."
                    sleep DELAY_ON_TABLE_DROP
                rescue Timeout::Error
                    info "timeout, give up leadership. current role: worker"
                    giveup_leadership
                rescue Aws::DynamoDB::Errors::ConditionalCheckFailedException
                    info "we've lost leadership. current role: worker"
                    @leader = false
                rescue Aws::DynamoDB::Errors::ServiceError => e
                    info "DynamoDB service error during leader update, giving up leadership: (#{e.class}): #{e.message}"
                    giveup_leadership
                end

                # write heartbeat record into dynamoDB
                private
                def leader_heartbeat
                    Timeout::timeout(config.leader_update_timeout) do
                        expiration_time = Time.now + config.leadership_validity_period

                        dynamo_db.update_item :table_name => config.table_name,
                            :key => {
                            'id' => LEADER_ELECTION_RECORD_NAME
                        },
                        :attribute_updates => {
                            'leader_timestamp' => {
                                :value  => expiration_time.to_f,
                                :action => 'PUT'
                            }
                        },
                        :expected => {
                            'leader_id' => {
                                :attribute_value_list => [
                                    daemon_id
                                ],
                                :comparison_operator => 'EQ'
                            }
                        }

                        @last_expiration_time = expiration_time
                    end
                end

                # check the dynamoDB as the worker
                private
                def worker_check
                    debug 'perform worker checking ...'
                    begin
                        heartbeat_record_attributes = heartbeat_record

                        if heartbeat_record_attributes["leader_timestamp"] == nil
                            raise LeaderElection::Errors::NilAttributeException
                        end
                        leader_expiration_time = Time.at heartbeat_record_attributes["leader_timestamp"].to_f
                        if Time.now > leader_expiration_time
                            elect_new_leader :last_timestamp => leader_expiration_time
                        end
                        unless @leader
                            former_counter_timestamp = heartbeat_record_attributes["counter_timestamp"]
                            if former_counter_timestamp
                                former_counter_timestamp = Time.at former_counter_timestamp.to_f
                                time_delta = Time.now - former_counter_timestamp
                                update_check_period :worker_num => heartbeat_record_attributes["worker_counter"].to_i if time_delta > config.worker_registration_interval && time_delta < config.worker_count_interval
                            end

                        end
                    rescue Aws::DynamoDB::Errors::ResourceNotFoundException
                        error "DynamoDB table not found. sleeping..."
                        sleep DELAY_ON_TABLE_DROP
                    rescue Aws::DynamoDB::Errors::ConditionalCheckFailedException
                        info "failed to be elected as leader. current role: worker"
                        backoff
                    rescue Aws::DynamoDB::Errors::InternalServerError
                        info "Cannot talk to DynamoDB, because of InternalServerError"
                        demote
                    rescue LeaderElection::Errors::NilAttributeException
                        info "leader election record attribute not found, inserting"
                        init_heartbeat_record
                    rescue Aws::DynamoDB::Errors::ProvisionedThroughputExceededException
                        info "throttling exception during worker check"
                    end
                end

                # use conditional write in dynamoDB to elect a new leader
                private
                def elect_new_leader(last_timestamp:)
                    info "leader expired at #{last_timestamp}, electing new leader ..."
                    @last_expiration_time = Time.now + config.leadership_validity_period

                    dynamo_db.update_item :table_name => config.table_name,
                        :key => {
                        'id' => LEADER_ELECTION_RECORD_NAME
                    },
                    :attribute_updates => {
                        'leader_id' => {
                            :value  => daemon_id,
                            :action => 'PUT'
                        },
                        'leader_timestamp' => {
                            :value  => @last_expiration_time.to_f,
                            :action => 'PUT'
                        }
                    },
                    :expected => {
                        'leader_timestamp' => {
                            :attribute_value_list => [
                                last_timestamp.to_f
                            ],
                            :comparison_operator => 'EQ'
                        }
                    }

                    info "we're now the leader"
                    @leader = true
                end

                # backoff for a random time according to work check period in order to let works be asynchronized
                private
                def backoff
                    time = Random.new.rand(config.worker_check_interval.to_f)
                    debug "backoff for #{time} seconds"
                    sleep time
                end

                # reset the leadership to let other instances get the leadership quickly.
                private
                def giveup_leadership
                    if @leader
                        begin
                            reset_timestamp
                        rescue => e
                            AWS::EB::LeaderElection.format_exception(e)
                        ensure
                            @leader = false
                        end
                    end
                end

                # require a worker count
                private
                def worker_count
                    debug "require workers to add to the counter"

                    item = dynamo_db.update_item :table_name => config.table_name,
                        :key => {
                        'id' => LEADER_ELECTION_RECORD_NAME
                    },
                    :attribute_updates => {
                        'counter_timestamp' => {
                            :value  => Time.now.to_f,
                            :action => 'PUT'
                        },
                        'worker_counter' => {
                            :value  => 0,
                            :action => 'PUT'
                        }
                    },
                    return_values: 'ALL_OLD'

                    counter_timestamp = item.attributes["counter_timestamp"] || 0
                    valid_worker_data = (Time.now.to_f - counter_timestamp) < (config.worker_count_interval + config.tolerant_clock_difference)
                    valid_worker_data ? item.attributes["worker_counter"] : 0
                rescue Aws::DynamoDB::Errors::ServiceError => e
                    warn "DynamoDB service error while retrieving worker count:  (#{e.class}): #{e.message}"
                    0
                end

                # write the check new_period
                private
                def update_check_period(worker_num:)
                    if worker_num > 0
                        new_period = config.single_worker_check_interval * worker_num
                        unless config.worker_check_interval == new_period
                            config.worker_check_interval = new_period
                            debug "worker check period updated to #{config.worker_check_interval}"
                            backoff
                        end
                    end
                end

                # let workers to add count to counter
                private
                def register_as_worker
                    newest_request_time = counter_timestamp
                    unless newest_request_time.to_f == @last_count_time.to_f
                        dynamo_db.update_item :table_name => config.table_name,
                            :key => {
                            'id' => LEADER_ELECTION_RECORD_NAME
                        },
                        :attribute_updates => {
                            'worker_counter' => {
                                :value  => 1,
                                :action => 'ADD'
                            }
                        }

                        @last_count_time = Time.at newest_request_time.to_f
                    end
                rescue Aws::DynamoDB::Errors::ServiceError => e
                    warn "DynamoDB service error while registering as worker: (#{e.class}): #{e.message}"
                end

                private
                def counter_timestamp
                    heartbeat_record["counter_timestamp"]
                end

                private
                def heartbeat_record
                    r = dynamo_db.get_item :table_name => config.table_name,
                        :key => {
                        'id' => LEADER_ELECTION_RECORD_NAME
                    },
                    consistent_read: true
                    r.item.to_h
                end

                # test get_item permission on table
                private
                def read_registration
                    r = dynamo_db.get_item :table_name => config.table_name,
                                           :key => {
                                               'id' => REGISTRATION_RECORD_NAME
                                           },
                                           consistent_read: false
                    r.item.to_h
                end

                private
                def dynamo_db
                    @dynamo_db ||= Aws::DynamoDB::Client.new :region             => config.region,
                        :simple_attributes  => true,
                        :retry_limit        => 5
                end
            end
        end
    end
end
