module EM
  module Sequel
    class Mutex

      class SynchronizationError < StandardError; end

      def initialize
        @waiting = []
        @current = nil
      end
      
      def synchronize
        if @current
          if @current == Fiber.current
            raise SynchronizationError, "Mutex is already in synchronize" 
          else
            @waiting << Fiber.current
            Fiber.yield
          end
        end
        
        @current = Fiber.current

        begin
          yield
        ensure
          @current = nil
          if waiting = @waiting.shift
            waiting.resume
          end
        end
      end
    end
  end
end
