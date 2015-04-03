
module AWS
    module EB
        module SQSD
            class FatalError < RuntimeError; end

            # Dump error message and stack trace from exception
            # stack_trace: when set to false only exception messages are dumped
            # full_trace: when set to false only root cause exception trace is dumped, otherwise trace of every
            # nested exception is dumped
            def self.format_exception(e, stack_trace: false, full_trace: false)
                message = ""
                top_exception = true
                while e
                    if top_exception
                        top_exception = false
                    else
                        message << %[caused by: ]
                    end

                    message << %[#{e.message} (#{e.class})\n]
                    if stack_trace
                        first_trace = e.backtrace.first
                        message << %[\tat #{first_trace}\n]

                        if full_trace || e.cause.nil?
                            backtrace = e.backtrace.drop(1).collect { |i| "\tfrom #{i}"}.join("\n")
                            message << %[#{backtrace}\n]
                        else
                            message << %[\t...\n]
                        end
                    end

                    e = e.cause
                end

                %[#{message}\n\n]
            end

        end
    end
end