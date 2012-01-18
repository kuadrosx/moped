require "socket"

def connect_to(host, port)
  addr = Socket.getaddrinfo(host, nil)
  sock = Socket.new(Socket.const_get(addr[0][0]), Socket::SOCK_STREAM, 0)

  timeout = 0.5

  if timeout
    secs = Integer(timeout)
    usecs = Integer((timeout - secs) * 1_000_000)
    optval = [secs, usecs].pack("l_2")
    sock.setsockopt Socket::SOL_SOCKET, Socket::SO_RCVTIMEO, optval
    sock.setsockopt Socket::SOL_SOCKET, Socket::SO_SNDTIMEO, optval
  end

  sockaddr = Socket.pack_sockaddr_in(port, addr[0][3])

  begin
    sock.connect_nonblock sockaddr
  rescue IO::WaitWritable
    puts $!

    IO.select nil, [sock], nil, timeout

    begin
      sock.connect_nonblock sockaddr
    rescue Errno::EISCONN
    end
  end

  puts "CONNECTED!"
end

connect_to("46.38.190.67", 27017)
