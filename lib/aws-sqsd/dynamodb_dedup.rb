require 'zlib'
require 'aws-sdk'
require 'aws-sqsd/dynamodb_utils'

module AWS::EB::SQSD::DynamoDBDedup
    include AWS::EB::SQSD::DynamoDBUtilities

    @@partition_count = 10

    def process_message?(msg, start_time=nil)
        try(:retries => 0, :on_error => false) do
            begin
                store_message_item msg, :status => 'processing', :start_time => start_time, :unless_exists => true
                true
            rescue AWS::DynamoDB::Errors::ConditionalCheckFailedException
                # The message is either duplicate or "orphan"
                time, status = nil, nil
                try { time, status = retrieve_message_item(msg).attributes.values_at(:time, :status) }

                if time && status
                    visibility_timed_out = ( Time.now.to_f - time ) > visibility_timeout

                    if status == 'processing' && visibility_timed_out
                        log 'dedup', %[#{msg.id} failed earlier. reprocessing], start_time
                        true
                    elsif status == 'processing' && ! visibility_timed_out
                        log 'dedup', %[#{msg.id} is in progress. skipping], start_time
                        false
                    else
                        log 'dedup', %[#{msg.id} is a duplicate], start_time
                        counters.increase :dup_count
                        delete_message msg, start_time
                        false
                    end
                else
                    log 'dedup-err', %[failed to determine dedup status of #{msg.id}. skipping], start_time
                    false
                end
            end
        end
    end

    def message_processed(msg, start_time=nil)
        defer(:retries => 3) do
            store_message_item msg, :status => 'processed', :start_time => start_time
        end
    end

    def message_failed(msg, start_time=nil)
        defer(:retries => 3) do
            log 'dedup', %[#{msg.id} failed], start_time if config.verbose
            delete_message_item msg
        end
    end

    private
    def partition(msg)
        Zlib.crc32(msg.id) % @@partition_count
    end

    private
    def store_message_item(msg, status: nil, start_time: nil, unless_exists: false)
        raise %[status is required] unless status
        raise %[start_time is required] unless start_time

        event_start_time = start_time.kind_of?(Array) ? start_time.last : start_time
        event_start_time ||= Time.now

        retries = 10
        item_data = {}
        item_data[:partition] = partition msg
        item_data[:id] = dynamo_range_key(msg)
        item_data[:status] = status
        item_data[:time] = event_start_time.to_f
        item_data[:ttl] = Time.now.utc.to_i + config.retention_period

        counters.increase :concurrent_dups

        begin
            if unless_exists
                table(config.dedup_table).items.put item_data, :unless_exists => 'id'
            else
                table(config.dedup_table).items.put item_data
            end
        rescue *config.fatal_exceptions => e
            fatal e
        rescue AWS::DynamoDB::Errors::ConditionalCheckFailedException
            raise
        rescue AWS::DynamoDB::Errors::ProvisionedThroughputExceededException
            log 'dedup-err', %[dynamodb table #{config.dedup_table} exceeded throughput capacity]
            sleep 0.25
            retry if (retries -= 1) >= 0
        rescue RuntimeError => e
            log 'dedup-err', %[#{e.class}: #{e.message}]
            retry if (retries -= 1) >= 0
        end

        counters.decrease :concurrent_dups
        log 'dedup', %[#{msg.id} #{status}], start_time if config.verbose
        return false
    end

    private
    def retrieve_message_item(msg)
        try { table(config.dedup_table).items.at(partition(msg), dynamo_range_key(msg)) }
    end

    private
    def delete_message_item(msg)
        try { table(config.dedup_table).items.at(partition(msg), dynamo_range_key(msg)).delete }
    end

    private
    def dynamo_range_key(msg)
        %[#{msg.id}-#{config.queue_url}]
    end

    private
    def visibility_timeout
        @visibility_timeout ||= ( config.visibility_timeout || @queue.visibility_timeout )
    end
end
