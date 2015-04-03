require 'aws-sqsd/logger'


module AWS
    module EB
        module SQSD
            class BridgeLogger
                include Logger

                def info(message='')
                    log "leader-election", "#{message} #{yield}"
                end

                def warn(message='')
                    log "leader-election", "#{message} #{yield}"
                end

                def error(message='')
                    log "leader-election", "#{message} #{yield}"
                end

                def debug(message='')
                    debug_log "leader-election", "#{message} #{yield}"
                end

                def fatal(message='')
                    log "leader-election", "#{message} #{yield}"
                end

            end
        end
    end
end
