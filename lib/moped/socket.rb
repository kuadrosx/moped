module Moped
  class Socket

    class Connection < EM::Connection
      def self.connect(host, port)
        f = Fiber.current
        socket = EventMachine.connect(host, port, self)
        EM.next_tick { f.resume }
        Fiber.yield
        socket
      end

      def post_init
        @buffer = StringIO.new
        @callbacks  = {}
        @request_id = 0
      end

      def next_request_id
        @request_id += 1
      end

      def send_command(op, callback = nil)
        op.request_id = next_request_id

        @callbacks[op.request_id] = callback if callback
        send_data op.serialize
      end

      def receive_data(data)
        if @buffer.length == 0
          @buffer.string.replace(data)
        else
          @buffer.string << data
        end

        while reply = parse_reply(@buffer)
          callback = @callbacks.delete(reply.response_to)
          callback.call(reply) if callback
        end

        if @buffer.eof?
          @buffer.string.clear
          @buffer.rewind
        end
      end

      def parse_reply(buffer)
        remaining_bytes = buffer.length - buffer.pos

        return nil unless remaining_bytes > 36
        length, = buffer.read(4).unpack('l<')

        unless remaining_bytes >= length - 4
          buffer.pos -= 4
          # buffer.seek(-4, IO::SEEK_CUR)
          return nil
        end

        reply = Protocol::Reply.allocate

        reply.length = length

        reply.request_id,
          reply.response_to,
          reply.op_code,
          reply.flags,
          reply.cursor_id,
          reply.offset,
          reply.count = buffer.read(32).unpack('l4<q<l2<')

        documents = reply.documents = []
        count = reply.count
        i = 0

        while i < count
          documents << BSON::Document.deserialize(buffer)
          i += 1
        end

        reply
      end

      def closed?
        false
      end

    end

    # Thread-safe atomic integer.
    class RequestId
      def initialize
        @mutex = Mutex.new
        @id = 0
      end

      def next
        @mutex.synchronize { @id += 1 }
      end
    end

    attr_reader :connection

    attr_reader :host
    attr_reader :port

    def initialize(host, port)
      @host = host
      @port = port

      @mutex = Mutex.new
      @request_id = RequestId.new
    end

    def connect
      return true if @connected

      @connection = Connection.connect host, port
      @connected = true
    end

    # Execute the operation on the connection. Pass a callback if you're
    # interested in the results.
    def execute(op)
      if Protocol::Query === op || Protocol::GetMore === op
        f = Fiber.current
        connection.send_command op, ->(reply) {
          f.resume reply
        }
        Fiber.yield
      else
        connection.send_command op
        true
      end
    end

    def parse_reply(length, data)
      buffer = StringIO.new data

      reply = Protocol::Reply.allocate

      reply.length = length

      reply.request_id,
        reply.response_to,
        reply.op_code,
        reply.flags,
        reply.cursor_id,
        reply.offset,
        reply.count = buffer.read(32).unpack('l4<q<l2<')

      reply.documents = reply.count.times.map do
        BSON::Document.deserialize(buffer)
      end

      reply
    end

    # Executes a simple (one result) query and returns the first document.
    #
    # @return [Hash] the first document in a result set.
    def simple_query(query)
      reply = execute(query)
      reply.documents.first
    end

    # @return [Boolean] whether the socket is dead
    def dead?
      @mutex.synchronize do
        @dead || @connection.closed?
      end
    end

    # Manually closes the connection
    def close
      @mutex.synchronize do
        return if @dead

        @dead = true
        @connection.close unless @connection.closed?
      end
    end

  end
end
