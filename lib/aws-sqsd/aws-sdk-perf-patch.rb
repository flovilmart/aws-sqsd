require 'aws-sdk'

# https://github.com/aws/aws-sdk-ruby/commit/a1c0197977560bc72d2eb0a876daa63648b2d985
class AWS::Core::CredentialProviders::EC2Provider
    def credentials
        if @credentials_expiration && @credentials_expiration.utc <= (Time.now.utc + (15 * 60))
            refresh
        end
        super
    end
end

module AWS::Core::OptionGrammar::Descriptors::ListMethods::InstanceMethods
    def validate(value, context = nil)
        true
    end
end

module AWS::Core::Inflection
    def ruby_name(aws_name)
        @@ruby_name_cache ||= {}
        return @@ruby_name_cache[aws_name] if @@ruby_name_cache[aws_name]

        inflector = Hash.new do |hash,key|

          key.
            sub(/^.*:/, '').                          # strip namespace
            gsub(/([A-Z0-9]+)([A-Z][a-z])/, '\1_\2'). # split acronyms
            scan(/[a-z]+|\d+|[A-Z0-9]+[a-z]*/).       # split words
            join('_').downcase                        # join parts

        end

        # add a few irregular inflections
        inflector['ETag'] = 'etag'
        inflector['s3Bucket'] = 's3_bucket'
        inflector['s3Key'] = 's3_key'
        inflector['Ec2KeyName'] = 'ec2_key_name'
        inflector['Ec2SubnetId'] = 'ec2_subnet_id'
        inflector['Ec2VolumeId'] = 'ec2_volume_id'
        inflector['Ec2InstanceId'] = 'ec2_instance_id'
        inflector['ElastiCache'] = 'elasticache'
        inflector['NotificationARNs'] = 'notification_arns'

        @@ruby_name_cache[aws_name] = inflector[aws_name]
    end
    module_function :ruby_name
end

module AWS::Core::UriEscape
    def escape value
        @@uri_escape_cache ||= {}
        return @@uri_escape_cache[value] if @@uri_escape_cache[value]

        value = value.encode("UTF-8") if value.respond_to?(:encode)
        escaped_value = CGI::escape(value.to_s).gsub('+', '%20').gsub('%7E', '~')
        @@uri_escape_cache[value] = escaped_value if value == escaped_value || ( value.respond_to?(:start_with?) && value.start_with?('http'))
        escaped_value
    end
end

class AWS::Core::Signers::Version4
    def authorization(req, key, datetime, content_sha256)
        signature_key = []
        signature_key << region
        signature_key << service_name
        signature_key << req.path
        signature_key << req.http_method
        signature_key << req.querystring

        cache_validation_token = []
        cache_validation_token << req.headers["x-amz-content-sha256"]
        cache_validation_token << credentials
        cache_validation_token << datetime

        @@signature_cache ||= {}
        if auth = @@signature_cache[signature_key]
            if auth[0] == cache_validation_token
                return auth[1]
            end
        end

        parts = []
        parts << "AWS4-HMAC-SHA256 Credential=#{credential(datetime)}"
        parts << "SignedHeaders=#{signed_headers(req)}"
        parts << "Signature=#{signature(req, key, datetime, content_sha256)}"
        auth = parts.join(', ')

        cached_entry = []
        cached_entry << cache_validation_token
        cached_entry << auth
        @@signature_cache[signature_key] = cached_entry

        auth
    end
end
