require 'ostruct'

require 'leader_election/exceptions'

module AWS
    module EB
        module LeaderElection
            class Config < OpenStruct

                INSTANCE_DOCUMENT_URL = 'http://169.254.169.254/latest/dynamic/instance-identity/document'
                DEFAULTS_CONFIG = {
                    :leader_update_interval => 5,
                    :worker_check_interval => 1,
                    :worker_count_interval => 30,
                    :worker_registration_interval => 10,
                    :leader_update_timeout => 2,
                    :leadership_validity_period => 10,
                    :tolerant_clock_difference => 1,
                    :rest_time => 15,
                    :workercount => false,
                    :single_worker_check_interval => 1
                }


                def initialize(options={})
                    # required parameters
                    raise AWS::EB::LeaderElection::FatalException %[:tale_name is required] unless options.has_key?(:table_name)

                    options = set_defaults(options)
                    super(options)
                end


                def region
                    instance_doc.fetch('region')
                end


                def instance_id
                    instance_doc.fetch('instanceId')
                end


                private
                def set_defaults(options)
                    options = DEFAULTS_CONFIG.merge(options)
                end


                private
                def instance_doc
                    @instance_doc_hash ||= JSON.load(open(INSTANCE_DOCUMENT_URL).read)
                end

            end
        end
    end
end
