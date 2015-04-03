require 'aws-sdk'

class AWS::SQS::ReceivedMessage
	# @return [String] The message attributes attached to the message.
	attr_reader :message_attributes

	# @api private
	def initialize(queue, id, handle, opts = {})
		@queue = queue
		@id = id
		@handle = handle
		@body = opts[:body]
		@md5 = opts[:md5]
		@attributes = opts[:attributes] || {}
		@message_attributes = opts[:message_attributes] || {}
		super
	end      
end

class AWS::SQS::Queue
	def receive_message(opts = {}, &block)
		resp = client.receive_message(receive_opts(opts))

		failed = verify_receive_message_checksum resp

		raise Errors::ChecksumError.new(failed) unless failed.empty?

		messages = resp[:messages].map do |m|
			AWS::SQS::ReceivedMessage.new(self, m[:message_id], m[:receipt_handle],
			:body => m[:body],
			:md5 => m[:md5_of_body],
			:attributes => m[:attributes],
			:message_attributes => m[:message_attributes])
		end

		if block
			call_message_block(messages, block)
		elsif opts[:limit] && opts[:limit] != 1
			messages
		else
			messages.first
		end
	end

	# @api private
	protected
	def receive_opts(opts)
		receive_opts = { :queue_url => url }
		receive_opts[:visibility_timeout] = opts[:visibility_timeout] if
							opts[:visibility_timeout]
		receive_opts[:max_number_of_messages] = opts[:limit] if
							opts[:limit]
		receive_opts[:wait_time_seconds] = opts[:wait_time_seconds] if
							opts[:wait_time_seconds]

		if names = opts[:attributes]
			receive_opts[:attribute_names] = names.map do |name|
				name = AWS::SQS::ReceivedMessage::ATTRIBUTE_ALIASES[name.to_sym] if
				AWS::SQS::ReceivedMessage::ATTRIBUTE_ALIASES.key?(name.to_sym)
				name = AWS::Core::Inflection.class_name(name.to_s) if name.kind_of?(Symbol)
				name
			end
		end

		if names = opts[:message_attributes]
			receive_opts[:message_attribute_names] = names.map do |name|
				name = AWS::SQS::ReceivedMessage::ATTRIBUTE_ALIASES[name.to_sym] if
				AWS::SQS::ReceivedMessage::ATTRIBUTE_ALIASES.key?(name.to_sym)
				name = AWS::Core::Inflection.class_name(name.to_s) if name.kind_of?(Symbol)
				name
			end
		end  

		receive_opts
	end
end

