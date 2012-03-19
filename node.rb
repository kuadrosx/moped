class Node

  def initialize
    @server = TCPServer.new "127.0.0.1", 0
    @port = @server.addr[1]
    @address = @server.addr[2]

    start
  end

  def recover!
    @server = TCPServer.new @address, @port

    start
  end

  def die!
    @server.close
    @thread.kill
  end

  def hiccup!
    @socket.close
  end

  def start
    while socket = @server.accept
      # receive message

      if insert
        # ...
      elsif command
        if command == ismaster?
          ack :is_master
        elsif command == getlasterror
          ack :success
        end
      end
    end
  end

end
