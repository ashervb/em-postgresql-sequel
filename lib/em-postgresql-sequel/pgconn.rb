# = pgconn.rb
# * callback and errback do the same thing.  If that is expected, use
# the same block for both.
# * There's no reason for the module Sequel module Postgres stuff if you
# are just reopening ::PGconn.

module Sequel
  module Postgres
    class ::PGconn
      def async_exec(sql, args=nil)
        send_query(sql, args)

        deferrable = ::EM::DefaultDeferrable.new
        ::EM.watch(self.socket, EM::Sequel::Postgres::Watcher, self, deferrable).notify_readable = true
        
        f = Fiber.current
        
        deferrable.callback do |res| 
          # check for alive?, otherwise we probably resume a dead fiber, because someone has killed our session e.g. "select pg_terminate_backend('procpid');"
          f.resume(res) if f.alive?
        end
        
        deferrable.errback  do |err| 
          # check for alive?, otherwise we probably resume a dead fiber, because someone has killed our session e.g. "select pg_terminate_backend('procpid');"
          f.resume(err) if f.alive?
        end
    
        Fiber.yield.tap do |result|
          raise result if result.is_a?(Exception)
        end
        
      end
    end
  end
end
