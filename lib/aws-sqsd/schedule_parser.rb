
require 'executor'
require 'parse-cron'
require 'yaml'

require 'aws-sqsd/exception'
require 'aws-sqsd/logger'

#
# Required module: AWS::EB::SQSD
#
module AWS::EB::SQSD::ScheduleParser

    class ScheduleFileError < RuntimeError; end

    class ScheduleFile
        include AWS::EB::SQSD::Logger

        SchedulerJob = Struct.new(:parser, :path)

        SCHEDULE_FILE_NAME = 'cron.yaml'
        GETCONFIG_CMD = '/opt/elasticbeanstalk/bin/get-config container -k app_deploy_dir'
        APP_PATHS = [
            '/var/lib/tomcat7/webapps/ROOT',
            '/var/app/current/',
        ]

        PARSER_VERSION_MAP = {
            1 => :v1_parser,
        }
        LOCALHOST_URL = 'http://localhost/' # only used to validate url path

        SEND_EVENT_CMD = "eventHelper.py --msg='%{msg}'"

        public
        def load_schedule
            file_path = schedule_file_path

            # If schedule file exists, parse and return jobs. Otherwise return empty hash.
            if file_path
                begin
                    content = YAML.load(File.read(file_path))
                    version = content.fetch('version')
                rescue NoMethodError => e
                    raise ScheduleFileError.new("Schedule file '#{SCHEDULE_FILE_NAME}' is empty.")
                rescue KeyError => e
                    raise ScheduleFileError.new("Cannot find 'version' attribute from schedule file '#{SCHEDULE_FILE_NAME}'")
                rescue => e
                    raise ScheduleFileError.new("Failed of loading file '#{SCHEDULE_FILE_NAME}', because: #{e.message}")
                end

                parser = PARSER_VERSION_MAP[version]
                if parser
                    jobs = self.send(parser, content)   # In case of parsing error, just let exception pop back
                    log('schedule-parser', "Successfully loaded #{jobs.length} scheduled tasks from file #{file_path} .")
                    event_msg = Executor::Exec.sh(SEND_EVENT_CMD % {
                            :msg => "Successfully loaded #{jobs.length} scheduled tasks from #{SCHEDULE_FILE_NAME}."},
                        raise_on_error: false)
                    debug_log('schedule-parser', event_msg)
                    return jobs
                else
                    raise ScheduleFileError.new("Not supported version '#{version}' for schedule file '#{file_path}'")
                end

            else
                debug_log('schedule-parser', "cannot locate file '#{SCHEDULE_FILE_NAME}'. Proceeding as it's not specified.")
                return {}
            end
        end


        private
        def v1_parser(content)
            jobs = {}
            begin
                content.fetch('cron').each do |task_def|
                    name = task_def.fetch('name')

                    # retrieve and test url path
                    path = task_def.fetch('url')
                    URI.join(LOCALHOST_URL, path)

                    # retrieve and test schedule string
                    schedule = task_def.fetch('schedule')
                    parser = CronParser.new(schedule)
                    parser.next(Time.now) # test by computing next scheduled time
                    parser.last(Time.now) # test by computing last scheduled time

                    scheduled_job = SchedulerJob.new(parser, path)
                    jobs[name] = scheduled_job
                end
            rescue => e
                raise ScheduleFileError.new("Failed of parsing file '#{SCHEDULE_FILE_NAME}', because: #{e.message}")
            end
            jobs
        end


        # Try to locate schedule file
        private
        def schedule_file_path
            # Try get-config first
            app_path = nil
            begin
                app_path = Executor::Exec.sh(GETCONFIG_CMD)
            rescue => e
                debug_log('schedule-parser', "failed to retrieve app_deploy_path using get-config, because #{e.message}")
            end

            # enumerate possible paths (dropping app_path if it's nil)
            [app_path, *APP_PATHS].compact.each do |app_path|
                file_path = File.join(app_path, SCHEDULE_FILE_NAME)
                return file_path if File.exist?(file_path)
            end

            # cannot find a valid path
            nil
        end

    end

    public
    def self.load_schedule
        return ScheduleFile.new.load_schedule
    end

end
