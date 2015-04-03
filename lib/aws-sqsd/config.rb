
require 'json'
require 'open-uri'
require 'ostruct'
require 'yaml'

require 'aws-sqsd/exception'


module AWS
    module EB
        module SQSD
            class Config < OpenStruct

                INSTANCE_DOCUMENT_URL = 'http://169.254.169.254/latest/dynamic/instance-identity/document'
                DEFAULTS_CONFIG = {

                    # local worker defaults
                    :http_url => 'http://localhost',
                    :http_path => '/',
                    :http_port => 80,
                    :threads => 30,
                    :http_connections => 16,
                    :mime_type => 'application/json',
                    :keepalive => true,
                    :connect_timeout => 5,
                    :trap => true,

                    # SQS defaults
                    :sqs_ssl => true,
                    :visibility_timeout => 300,
                    :inactivity_timeout => 300,
                    :sqs_verify_checksums => true,
                    :dedup => false,
                    :via_sns => false,
                    :error_visibility_timeout => false,
                    :retention_period => 345600, # 4 days
                    :sqs_batch_delete => true,
                    :sqs_batch_delete_size => 10,
                    :sqs_batch_receive_size => 10,
                    :batch_delete_timer => 2,
                    :poll_timer => 0.15,
                    :drain_timeout => 600,
                    :idle_mode_delay => 30,
                    :backlog_process_time => 1,

                    # DDB defaults
                    :dedup_table => nil,
                    :dynamodb_ssl => true,


                    # auto tuning defaults
                    :backlog_size => false,
                    :autotuning_threshold_slow => 1,
                    :autotuning_threshold_slowest => 15,
                    :sqs_wait_time_seconds => 20,
                    :sqs_max_wait_time_seconds => 20,
                    :sqs_max_batch_size => 10,
                    :current_sqs_max_batch_size => 10,
                    :poll_mode_switch_interval => 10,

                    # Cron schedule defaults
                    :record_message_retries => 1,
                    :hole_detection_retries => 1,
                    :message_resend_grace_period => 300,

                    # misc
                    :verbose => false,
                    :debug => false,
                    :quiet => false,

                    :environment_name => 'n/a',
                    :fatal_exceptions => [],
                }


                def initialize(options={})
                    # load parameters from config file
                    if file = options[:config_file]
                        unless File.exists? file
                            raise AWS::EB::SQSD::FatalError, %[Cannot load config file. No such file or directory: "#{file}"]
                        end

                        loaded_hash = YAML.load_file File.expand_path(file)
                        loaded_options = loaded_hash.inject({}){|h,(k,v)| h[k.to_sym] = v; h}   # cover key from string to symbol
                        options = loaded_options.merge options
                    end

                    # required parameters
                    raise AWS::EB::SQSD::FatalError, %[:queue_url is required] unless options.has_key? :queue_url

                    options = set_defaults(options)
                    super(options)
                end

                def region
                    instance_doc.fetch('region')
                end

                private
                def set_defaults(options)
                    # Pre processing before merging with default
                    options[:inactivity_timeout] ||= options[:visibility_timeout] if options[:visibility_timeout]
                    options[:lock_sqs_batch_delete] = true if options[:sqs_batch_delete]

                    options = DEFAULTS_CONFIG.merge(options)

                    # Post processing after merging with default
                    options[:error_visibility_timeout] = false if options[:error_visibility_timeout] && options[:error_visibility_timeout] < 0

                    options[:healthcheck] ||= %[TCP:#{options[:http_port]}]

                    options[:lock_concurrent_sqs_polls] = true if options[:concurrent_sqs_polls]
                    options[:concurrent_sqs_polls] ||= [options[:http_connections]/2.5, 1].max.ceil

                    options
                end

                private
                def instance_doc
                    @instance_doc_hash ||= JSON.load(open(INSTANCE_DOCUMENT_URL).read)
                end
            end
        end
    end
end
