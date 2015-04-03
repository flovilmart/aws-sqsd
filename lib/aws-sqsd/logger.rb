require 'fileutils'

module AWS::EB::SQSD::Logger

    DEFAULT_LOG_PATH = '/var/log/aws-sqsd/default.log'
    @@log_instance = nil


    def log(*args)
        AWS::EB::SQSD::Logger.log *args
    end

    def fatal(*args)
        AWS::EB::SQSD::Logger.fatal *args
    end

    def debug_log(*args)
        AWS::EB::SQSD::Logger.log *args if $debug || $DEBUG
    end

    def verbose_log(*args)
        AWS::EB::SQSD::Logger.log *args if $verbose || $VERBOSE
    end

    def self.log(category, message, start_times=nil)
        timestamp = Time.now.utc.iso8601
        line = %[#{timestamp} #{category}: #{message}]

        if start_times
            start_times = [start_times].flatten
            start_times.each { |start_time|
                line = %[#{line} - #{since_start(start_time)}]
            }
        end

        log_stream << %[#{line}\n]
    end

    def self.fatal(exception)
        log 'fatal', %[#{exception.class}: #{exception.message}]
#        FileUtils.rm_f $pid_file if $pid_file
        exit 1
    end

    private
    def self.since_start(start_time)
        format "%3.3f", Time.now - start_time
    end

    private
    def self.log_stream
        @@log_instance ||= begin
            file_path = $log_file || DEFAULT_LOG_PATH
            f = File.new(file_path, 'a')
            f.sync = true
            f
        end
    end
end
