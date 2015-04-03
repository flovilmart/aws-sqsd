require 'aws-sdk'

# required methods: region
# required members: @config.dynamodb_ssl

# produce members: @tables, @dynamo_db

module AWS::EB::SQSD::DynamoDBUtilities
    private
    def table(name=nil)
        @tables ||= {}

        raise AWS::EB::SQSD::FatalError, %[DynamoDB table name is not specificed] unless name

        if table = @tables[name]
            table
        else
            @dynamo_db ||= AWS::DynamoDB.new :region  => region,
                                             :use_ssl => @config.dynamodb_ssl
            table = @dynamo_db.tables[name]
            raise AWS::EB::SQSD::FatalError, %[DynamoDB table "#{name}" does not exist] unless table.exists?
            table.load_schema
            @tables[name] = table
            table
        end
        table
    end
end
