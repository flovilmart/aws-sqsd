module AWS::EB::SQSD::Version
    @@sqsd_version = %[2.0]
    @@sqsd_full_version = %[2.0 (2015-02-18)]

    def version
        AWS::EB::SQSD::Version.version
    end

    def full_version
        AWS::EB::SQSD::Version.full_version
    end

    def self.version
        @@sqsd_version
    end

    def self.full_version
        @@sqsd_full_version
    end
end
