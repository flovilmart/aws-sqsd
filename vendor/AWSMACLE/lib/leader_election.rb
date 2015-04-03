$LOAD_PATH.unshift File.expand_path('.', __dir__)
require 'leader_election/daemon'

module AWS
    module EB
        module LeaderElection
            def self.create(options={})
                Daemon.new options
            end
        end
    end
end
