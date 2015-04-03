module AWS
    module EB
        module LeaderElection
            module LoggerWriter
                private
                def debug(content)
                    @logger.debug { content }
                end

                private
                def info(content)
                    @logger.info { content }
                end

                private
                def warn(content)
                    @logger.warn { content }
                end

                private
                def error(content)
                    @logger.error { content }
                end

                private
                def fatal(content)
                    @logger.fatal { content }
                end
            end
        end
    end
end
