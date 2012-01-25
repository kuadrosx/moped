require "test/unit"
require "socket"

class Connection
  attr_reader :host, :port

  def initialize(host, port)
    @host, @port = host, port
  end

  def connection
    @connection
  end

  def connected?
    !!@connection
  end

  def connect
    return true if connection

    socket = ::Socket.new ::Socket::AF_INET, ::Socket::SOCK_STREAM, 0
    sockaddr = ::Socket.pack_sockaddr_in(port, host)

    begin
      socket.connect_nonblock sockaddr
    rescue IO::WaitWritable, Errno::EINPROGRESS
      IO.select nil, [socket], nil, 0.5

      begin
        socket.connect_nonblock sockaddr
      rescue Errno::EISCONN # we're connected
      rescue
        socket.close rescue nil # jruby raises EBADF
        return false
      end
    end

    @connection = socket
  end
end

class ConnectNonblockTest < Test::Unit::TestCase

  def setup
    @server = TCPServer.open "localhost", 33333
    @connection = Connection.new "localhost", @server.addr[1]
  end

  def teardown
    @server.close unless @server.closed?
    if @connection.connected?
      @connection.connection.close
    end
  end

  def test_success_case
    assert_nothing_raised do
      @connection.connect
    end

    assert @connection.connected?
  end

  def test_server_inaccessible
    @server.close

    assert_nothing_raised do
      @connection.connect
    end

    refute @connection.connected?
  end

  def test_server_timeout
    @server.listen(1)
    blocking_connection = TCPSocket.open "localhost", @server.addr[1]

    assert_nothing_raised do
      @connection.connect
    end

    refute @connection.connected?
  end

end
