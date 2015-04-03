require 'aws-sdk'
require 'timeout'
require 'open-uri'
require 'socket'
require 'net/http'

# TCP:80
# HTTP:80/weather/us/wa/seattle
#
module AWS::EB::SQSD::CloudWatchMonitor
    @@metric_namespace = 'ElasticBeanstalk/SQSD'
    @@metric_name = 'Health'
    @@cloudwatch_metric_interval = 60 # seconds
    @@healtcheck_protocols = %w[http tcp]

    def monitor
        @cloud_watch = AWS::CloudWatch.new :region => config.region
        @monitor_timer = EventMachine::PeriodicTimer.new(config.cloudwatch_interval || @@cloudwatch_metric_interval) do
            emit_metric(healthy? ? 1 : 0)
        end
    end

    private
    def emit_metric(health)
        metric_options = {
            :namespace => @@metric_namespace, 
            :metric_data => [
                {
                    :metric_name => @@metric_name, 
                    :value => health, 
                    :dimensions => [
                        { :name => 'EnvironmentName', :value => config.environment_name }
                    ]
                }
            ]
        }

        log 'metrics', %[emitting instance health: #{health}] if config.verbose
        try(log_category: "metrics", :retries => 0) do
            @cloud_watch.put_metric_data metric_options
        end
    end

    private
    def healthy?
        ok = false
        protocol, port, path = nil, nil, nil

        try(log_category: "metrics", :retries => 0) {
            protocol, port, path = parse_healthcheck config.healthcheck
        }

        if path
            uri = URI "#{protocol}://localhost:#{port}#{path}"
            begin
                # timeout
                #
                code = Net::HTTP.get_response(uri).code
                if code == "200"
                    ok = true
                else
                    log 'healthcheck-err', %[service healthcheck to URL "#{uri}" failed with http status code "#{code}"]
                end
            rescue Exception => e
                log 'healthcheck-err', %[service healthcheck failed with error: #{e.message}]
            end
        else
            begin
                socket = TCPSocket.new 'localhost', port
                ok = true
            rescue Exception
            ensure
                socket.close if socket && ! socket.closed?
            end
            log 'healthcheck-err', %[service healthcheck failed] unless ok
        end

        ok = ok && @successful_sqs_poll
        @successful_sqs_poll = false
        return ok
    end

    private
    def parse_healthcheck(target)
        protocol, port, path = target.scan(/(.+?):([0-9]+)(.*)/).first

        unless protocol && protocol
            raise AWS::EB::SQSD::FatalError, %[invalid healthcheck target "#{target}"]
        end

        path = nil if path && path.empty?
        protocol = protocol.downcase if protocol

        unless @@healtcheck_protocols.include? protocol
            raise AWS::EB::SQSD::FatalError, %[invalid protocol "#{protocol}". supported protocols are: #{@@healtcheck_protocols.join ' '}]
        end

        if protocol == 'tcp' && path
            raise AWS::EB::SQSD::FatalError, %[invalid healthcheck. path is only supported with 'http']
        end

        return [protocol, port, path]
    end
end
