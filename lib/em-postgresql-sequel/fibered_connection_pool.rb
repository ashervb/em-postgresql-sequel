module EM
  module Sequel
    
    class FiberedConnectionPool < ::Sequel::ConnectionPool

      # The maximum number of connections this pool will create.
      attr_reader :max_size
      
      # An array of connections that are available for use by the pool.
      attr_reader :available_connections
      
      # An array of connections that are waiting for use by the pool.
      attr_reader :waiting_connections

      def initialize(opts={}, &block)
        super

        @max_size = Integer(opts[:max_connections] || 4)
        raise(Sequel::Error, ':max_connections must be positive') if @max_size < 1

        @available_connections = []
        @waiting_connections   = []
        
        @mutex = Mutex.new
      end

      # Yield all of the available connections, and the one currently allocated to
      # this thread. This will not yield connections currently allocated to other
      # threads, as it is not safe to operate on them. This holds the mutex while
      # it is yielding all of the available connections, which means that until
      # the method's block returns, the pool is locked.
      def all_connections
        hold do |c|
          sync do
            yield c
            @available_connections.each{|c| yield c}
          end
        end
      end
      
      def disconnect(opts={}, &block)

        block ||= @disconnection_proc

        sync do
          @available_connections.each{|conn| block.call(conn)} if block
          @available_connections.clear
        end
      end

      def size
        @available_connections.length + @waiting_connections.length
      end
      
      def hold(server=nil)

        if @available_connections.empty? 
          if @waiting_connections.length < max_size
            @available_connections << make_new(DEFAULT_SERVER)
          else
            @waiting_connections << Fiber.current
            Fiber.yield
          end
        end

        @waiting_connections.delete Fiber.current
        
        conn = acquire

        begin
          yield conn
        rescue ::Sequel::DatabaseDisconnectError
          oconn = conn
          conn  = nil
          @disconnection_proc.call(oconn) if @disconnection_proc && oconn
          @available_connections << make_new(DEFAULT_SERVER)
          raise
        ensure
          sync do
            @available_connections << conn if conn
            if waiting_connections = @waiting_connections.shift
              waiting_connections.resume
            end
          end
        end
      end

      private

      # Assigns a connection to the supplied thread, if one
      # is available. The calling code should NOT already have the mutex when
      # calling this.
      def acquire
        sync do
          @available_connections.pop
        end
      end

      # Yield to the block while inside the mutex. The calling code should NOT
      # already have the mutex before calling this.
      def sync
        @mutex.synchronize{yield}
      end

    end

  end
end
