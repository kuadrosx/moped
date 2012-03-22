require "timeout"

module Moped
  class Connection

    class TCPSocket < ::TCPSocket
      def self.connect(host, port, timeout)
        Timeout::timeout(timeout) do
          new(host, port).tap do |sock|
            sock.set_encoding 'binary'
          end
        end
      end

      def read(length)
        result = ""
        readpartial length, result

        while (pending = length - result.bytesize) > 0
          chunk = ""
          readpartial pending, chunk
          result << chunk
        end

        result
      end

      def alive?
        if Kernel::select([self], nil, nil, 0)
          !eof? rescue false
        else
          true
        end
      end
    end

    def initialize
      @sock = nil
      @request_id = 0
    end

    def connect(host, port, timeout)
      @sock = TCPSocket.connect host, port, timeout
    end

    def alive?
      connected? ? @sock.alive? : false
    end

    def connected?
      !!@sock
    end

    def disconnect
      @sock.close
    rescue
    ensure
      @sock = nil
    end

    def write(operation)
      operation.request_id = (@request_id += 1)
      @sock.write operation
    end

    def read
      reply = Protocol::Reply.allocate

      reply.length,
        reply.request_id,
        reply.response_to,
        reply.op_code,
        reply.flags,
        reply.cursor_id,
        reply.offset,
        reply.count = @sock.read(36).unpack('l5<q<l2<')

      if reply.count == 0
        reply.documents = []
      else
        buffer = StringIO.new(@sock.read(reply.length - 36))

        reply.documents = reply.count.times.map do
          BSON::Document.deserialize(buffer)
        end
      end

      reply
    end
  end
end
