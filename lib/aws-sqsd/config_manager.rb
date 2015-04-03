require 'aws-sqsd/exception'
require 'ostruct'
require 'yaml'

module AWS
    module EB
        module SQSD
            class ConfigManager
                attr_accessor :config

                @@http_url = 'http://localhost'
                @@http_path = '/'
                @@http_port = 80
                @@threads = 30
                @@http_connections = 16
                @@sqs_ssl = true
                @@dynamodb_ssl = true
                @@mime_type = 'application/json'
                @@visibility_timeout = 300
                @@via_sns = false
                @@dedup = false
                @@sqs_batch_delete = true
                @@sqs_verify_checksums = true
                @@verbose = false
                @@debug = false
                @@quiet = false
                @@keepalive = true
                @@dedup_table = nil
                @@connect_timeout = 5
                @@inactivity_timeout = 180
                @@error_visibility_timeout = false;
                @@retention_period = 345600 # 4 days
               
                @@backlog_size = false
                @@autotuning_threshold_slow = 1
                @@autotuning_threshold_slowest = 15

                @@sqs_batch_delete_size = 10
                @@sqs_batch_receive_size = 10
                @@poll_timer = 0.15
                @@batch_delete_timer = 2
                @@sqs_wait_time_seconds = 20
                @@sqs_max_wait_time_seconds = 20
                @@drain_timeout = 600
                @@idle_mode_delay = 30
                @@backlog_process_time = 1
               
               
                @@az_url = 'http://169.254.169.254/latest/meta-data/placement/availability-zone'
                @@environment_name = 'n/a'


                def self.generate (options={})
            
                    if file = options[:config_file]
                        unless File.exists? file
                            raise AWS::EB::SQSD::FatalError, %[No such file or directory - #{file}]
                        end

                        loaded_options = YAML.load_file File.expand_path(file)
                        options = loaded_options.merge options
                    end

                    @config = OpenStruct.new(options)
                    @config = set_defaults(@config)
                    
                    @config
                end


                private
                def self.set_defaults(config)
                    config = config.clone
                    keys = config.to_h.keys

                    # required options
                    raise AWS::EB::SQSD::FatalError, %[queue_url is required] unless config.queue_url

                    # set defaults
                    config.http_path ||= @@http_path
                    config.http_port ||= @@http_port
                    config.threads ||= @@threads
                    config.http_connections ||= @@http_connections
                    config.mime_type ||= @@mime_type
        
                    config.dedup_table ||= @@dedup_table
                    config.environment_name ||= @@environment_name
                    config.connect_timeout ||= @@connect_timeout
                    config.error_visibility_timeout = @@error_visibility_timeout unless keys.include? :error_visibility_timeout
                    config.error_visibility_timeout = @@error_visibility_timeout if config.error_visibility_timeout && config.error_visibility_timeout < 0
                    config.visibility_timeout ||= @@visibility_timeout
                    config.inactivity_timeout ||= config.visibility_timeout                
                    config.retention_period ||= @@retention_period
                    config.drain_timeout ||= @@drain_timeout
                    config.backlog_size ||= @@backlog_size
                    config.autotuning_threshold_slow ||= @@autotuning_threshold_slow
                    config.autotuning_threshold_slowest ||= @@autotuning_threshold_slowest

                    config.keepalive = @@keepalive unless keys.include? :keepalive
                    
                    config.trap = true unless keys.include? :trap
                    config.sqs_ssl = @@sqs_ssl unless keys.include? :sqs_ssl
                    config.dynamodb_ssl = @@dynamodb_ssl unless keys.include? :dynamodb_ssl
                    config.via_sns = @@via_sns unless keys.include? :via_sns
                    config.dedup = @@dedup unless keys.include? :dedup
                    config.sqs_batch_delete = @@sqs_batch_delete unless keys.include? :sqs_batch_delete
                    config.sqs_verify_checksums = @@sqs_verify_checksums unless keys.include? :sqs_verify_checksums
                    config.verbose = @@verbose unless keys.include? :verbose
                    config.debug = @@debug unless keys.include? :debug
                    config.quiet = @@quiet unless keys.include? :quiet

                    config.queue_region ||= queue_region(config.queue_url)
                    config.healthcheck ||= "TCP:#{config.http_port}"

                    config.lock_sqs_batch_delete = true if (keys.include? :sqs_batch_delete) && (config.sqs_batch_delete == true)
                    config.lock_concurrent_sqs_polls = true unless config.concurrent_sqs_polls
                    config.concurrent_sqs_polls ||= [config.http_connections/2.5,1].max.ceil
                    config.sqs_batch_delete_size = @@sqs_batch_delete_size;
                    config.sqs_max_batch_size = @@sqs_batch_receive_size
                    config.poll_timer = @@poll_timer
                    config.batch_delete_timer = @@batch_delete_timer
                    config.sqs_wait_time_seconds = @@sqs_wait_time_seconds
                    config.sqs_max_wait_time_seconds = @@sqs_max_wait_time_seconds
                    config.idle_mode_delay = @@idle_mode_delay
                    config.az_url = @@az_url
                    config.http_url = @@http_url
                    config.current_sqs_max_batch_size = 10
                    config.poll_mode_switch_interval = 10

                    config
                end

                private
                def self.queue_region(queue)
                    url = queue.respond_to?(:url) ? queue.url : queue
                    region = url[/sqs\.(.*?)\.amazonaws\.com/, 1]
            
                    unless region
                        raise AWS::EB::SQSD::FatalError, %[invalid queue url "#{url}"]
                    end
                    region
                end

                private
                def self.region
                    @current_region ||= Timeout::timeout(1) { open(@@az_url).read.chop } rescue nil
                    @current_region ||= 'us-east-1'   # we're not on EC2
                end

            end
        end
    end
end
