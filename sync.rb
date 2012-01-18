@user_seeds = ["master", "secondary", "secondary"]

@seeds = @user_seeds.dup

def sync
  seeds = @seeds.dup.shuffle
  seen = {}

  sync_seed = ->(seed) {
    server = Server.new seed

    unless seen[server.resolved_address]
      seen[server.resolved_address] = true

      hosts = sync_server(server)

      hosts.each do |host|
        sync_seed[host]
      end

    end
  }

  seeds.each do |seed|
    sync_seed[seed]
  end

  true
end

def sync_server(server)
  [].tap do |hosts|
    socket = server.acquire_socket

    if info = socket.connect

      if info["ismaster"]
        server.promote
      end

      hosts.push *server["primary"], *server["hosts"], *server["passives"]

      merge_server(server)

    end
  end
ensure
  remove(server) if $!
end

def merge_server(server)
  # lock

  previous = servers[server]
  master = server.master?

  if previous == nil
    servers << server

    if master
      masters << server
    else
      slaves << server
    end
  else
    if master != previous.master?
      # now it's master
      masters << slaves.remove(previous)
    else
      slaves << masters.remove(previous)
    end

    previous.merge server
  end

end

class Server
  def merge(other)
    @master = other.master

    other.close
  end

  def close
    @sockets.each &:close
    @sockets = []
  end
end
