module Moped
  class Server

    # @return [String] the original host:port address provided
    attr_reader :address

    # @return [String] the resolved IP address
    attr_reader :ip

    # @return [String] the port
    attr_reader :port

    def initialize(address)
      @address = address
      @ip, @port = resolve_address address
    end

    def resolve_address(address)
      host, port = address.split ":"

      # Addrinfo.tcp(host, port)

      info, = ::Socket.getaddrinfo host, port, nil, ::Socket::SOCK_STREAM

      [info[3], info[1]]
    end

    def ==(other)
      self.class === other && ip == other.ip && port == other.port
    end
    alias eql? ==

    def hash
      [ip, port].hash
    end

  end
end
