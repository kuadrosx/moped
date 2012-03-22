require "timeout"

module Moped

  # @api private
  #
  # The internal class wrapping a socket connection.
  class Socket
    attr_reader :connection

    attr_reader :host
    attr_reader :port

    def initialize(host, port)
      @host = host
      @port = port
    end

    # @return [true, false] whether the connection was successful
    # @note The connection timeout is currently just 0.5 seconds, which should
    #   be sufficient, but may need to be raised or made configurable for
    #   high-latency situations. That said, if connecting to the remote server
    #   takes that long, we may not want to use the node any way.
    def connect
      return true if connection

      @connection = Connection.new
      @connection.connect(host, port, 0.5)
    rescue Errno::ECONNREFUSED, Timeout::Error
      return false
    end

    # @return [true, false] whether this socket connection is alive
    def alive?
      @connection.alive?
    end

    # Execute the operations on the connection.
    def execute(*ops)
      instrument(ops) do

        reply = nil
        ops.each do |op|
          connection.write op

          if Protocol::Query === op || Protocol::GetMore === op
            reply = connection.read
          end
        end
        reply
      end
    end

    # Executes a simple (one result) query and returns the first document.
    #
    # @return [Hash] the first document in a result set.
    def simple_query(query)
      query = query.dup
      query.limit = -1

      execute(query).documents.first
    end

    # Manually closes the connection
    def close
      connection.disconnect if connection && connection.connected?
      @connection = nil
    end

    def auth
      @auth ||= {}
    end

    def apply_auth(credentials)
      return if auth == credentials
      logouts = auth.keys - credentials.keys

      logouts.each do |database|
        logout database
      end

      credentials.each do |database, (username, password)|
        login(database, username, password) unless auth[database] == [username, password]
      end
    end

    def login(database, username, password)
      getnonce = Protocol::Command.new(database, getnonce: 1)
      result = simple_query getnonce

      raise Errors::OperationFailure.new(getnonce, result) unless result["ok"] == 1

      authenticate = Protocol::Commands::Authenticate.new(database, username, password, result["nonce"])
      result = simple_query authenticate
      raise Errors::OperationFailure.new(authenticate, result) unless result["ok"] == 1

      auth[database.to_s] = [username, password]
    end

    def logout(database)
      command = Protocol::Command.new(database, logout: 1)
      result = simple_query command
      raise Errors::OperationFailure.new(command, result) unless result["ok"] == 1
      auth.delete(database.to_s)
    end

    def instrument(ops)
      instrument_start = (logger = Moped.logger) && logger.debug? && Time.now
      yield
    ensure
      log_operations(logger, ops, Time.now - instrument_start) if instrument_start && !$!
    end

    def log_operations(logger, ops, duration)
      prefix  = "  MOPED: #{host}:#{port} "
      indent  = " "*prefix.length
      runtime = (" (%.1fms)" % duration)

      if ops.length == 1
        logger.debug prefix + ops.first.log_inspect + runtime
      else
        first, *middle, last = ops

        logger.debug prefix + first.log_inspect
        middle.each { |m| logger.debug indent + m.log_inspect }
        logger.debug indent + last.log_inspect + runtime
      end
    end

  end
end
