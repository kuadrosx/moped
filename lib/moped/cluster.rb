module Moped

  class Cluster
    def initialize(seeds, direct = false)
      @seeds = seeds
      @direct = direct
    end
  end

  class GOCluster

    def AcquireSocket(slave_ok, timeout)
      started = Time.now

      loop do
        @lock.synchronize do
          loop do
            log "Cluster has: %d masters and %d slaves" % [masters.length, slaves.length]
            if masters.any? || (slaves.any? && slave_ok)
              break
            end

            if timeout > 0 && Time.now - started > timeout
              raise "no reachable servers"
            end

            log "Waiting for servers to synchronize..."

            unless syncing?
              sync_servers
            end

            @server_synced.wait
          end

          if !slave_ok || slaves.empty?
            server = masters.sample
          else
            server = slaves.sample
          end
        end

        begin
          return server.acquire_socket
        rescue
          # Something's wrong with the server, so remove it and resume the
          # search for a connection.
          remove_server(server)
          sync_servers
        end
      end
    end

    def remove_server(server)
      @lock.synchronize do
        log "Removing server #{server.address} from cluster"
        servers.delete(server)
        masters.delete(server)
        slaves.delete(server)
      end
    end

    # Synchronize all servers in the cluster.
    def sync_servers
      synchronize do
        return if syncing?

        @syncing = true
        direct = direct?
      end

      _sync_servers(direct)
    end

    def _sync_servers(direct)
      log "[sync] Starting full topology synchronization..."

      known = known_addresses

      started, finished = 0
      done = Mutex.new
      m = Mutex.new

      done.lock
      seen = []

      sync = -> (addr) {
        m.synchronize { started += 1 }

        Thread.new do
          begin
            server = Server.new(addr)
            unless seen[server.address]
              seen[server.address] = true
              hosts = sync_server(server)

              unless direct
                hosts.each do |host|
                  sync[host]
                end
              end
            end
          ensure
            m.synchronize { finished += 1 }
          end
        end
      }

      known.each { |addr| sync[addr] }

      # wait for done to be, well, done.
      done.wait_until { started == finished && finished >= known.length }

      synchronize do
        log("[sync] Synchronization completed: ", masters.length,
          " master(s) and, ", slaves.length, " slave(s) alive.")

        if servers.any?
          @dynamic_seeds = servers.map(&:address)
        end

        @server_synced.broadcast

        if !direct? && masters.empty || servers.empty?
          log "[sync] No masters found. Resynchronizing"
          _sync_servers
        end
      end

      sleep 0.1
      synchronize do
        @syncing = false
        @server_synced.broadcast
      end

    end

    def known_addresses
      synchronize do
        seeds | dynamic_seeds | servers.map(&:address)
      end
    end

    def sync_server(server)
      socket = server.acquire_socket

      session = Session.new(self, socket, database: "admin")
      result = session.command ismaster: 1

      if result["ismaster"]
        server.master = true
      elsif result["secondary"]
        log "secondary server"
      else
        log "something else"
      end

      hosts = []
      unless result["primary"].blank?
        hosts << result["primary"]
      end

      hosts |= result["secondaries"]
      hosts |= result["passives"]

      session.close
      merge_server(server)

      hosts
    rescue
      remove_server(server)
    end

    def merge_server(server)
      synchronize do
        if servers.include? server
          # resync
        else
          servers.push server
          if server.master?
            masters.push server
          else
            slaves.push server
          end
        end
      end
    end

  end
end
