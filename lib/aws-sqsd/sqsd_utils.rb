require 'aws-sdk'
require 'aws-sqsd/exception'
require 'em-http-request'

module AWS::EB::SQSD::SQSDUtilities
	private
	def try(log_category:, retries: 5, on: StandardError, on_error: nil)
		begin
			yield
		rescue *on => e
			error_message = %[#{e.class}: #{e.message}]
			if $verbose
			   indent = 28.times.collect { ' ' }.join
			   formatted_backtrace = e.backtrace.collect { |i| %[#{indent}#{i}]}.join "\n"
			   error_message = %[#{error_message}\n#{formatted_backtrace}]
			end
			log log_category, error_message
			(retries -= 1) >= 0 ? retry : on_error
		end
	end

	private
	def defer(log_category: 'sqsd', retries: 0)
		EventMachine.defer do
			try(log_category: log_category, :retries => retries) do
				yield
			end
		end
	end

end