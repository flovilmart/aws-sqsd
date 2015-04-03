require 'thread'

class AWS::EB::SQSD::Counters
    def initialize
        @counters = {}
    end

    def increase(name, count=1)
        @counters[name] ||= 0
        @counters[name] += count
    end

    def decrease(name, count=1)
        @counters[name] ||= 0
        @counters[name] -= count
    end

    def [](name)
        @counters[name] || 0
    end

    def to_h
        @counters.clone
    end
end
