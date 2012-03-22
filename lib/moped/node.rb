module Moped
  class Node

    MESSAGES = {
      command: Protocol::Command,
      delete: Protocol::Delete,
      get_more: Protocol::GetMore,
      insert: Protocol::Insert,
      kill_cursors: Protocol::KillCursors,
      query: Protocol::Query,
      update: Protocol::Update
    }

    def refresh!
      info = command "admin", ismaster: 1

      @primary = true if info["ismaster"]
      @secondary = true if info["secondary"]
      @known_hosts = [].tap do |hosts|
        hosts << info["primary"] if info["primary"]
        hosts |= info["hosts"] if info["hosts"]
        hosts |= info["passives"] if info["passives"]
      end
    end

    def build_message(type, args)
      MESSAGES[type].new(*args)
    end

    def command(database, command)
      operation = build_message :command, [database, command]

      process operation do |reply|
        result = reply.documents[0]

        raise Errors::OperationFailure.new(
          operation, result
        ) if result["ok"] != 1 || result["err"] || result["errmsg"]

        return result
      end
    end

    def insert(*args)
      process build_message(:insert, *args)
    end

    def get_more()
      process build_message(:get_more, *args)
    end

    def query(*args)
      operation = build_message(:query, *args)
      process operation do |reply|
        if reply.flags.include? :query_failure
          raise Errors::QueryFailure.new(operation, reply.documents.first)
        end
      end
    end

    def send_safely(command, safety)
      send *command
      send :command, "admin", { getlasterror: 1 }.merge(safety)

      true
    end

    def process(operation)
      reply = nil

      logging(operation) do
        ensure_connected do
          connection.write operation

          reply = connection.read if READ_OPERATIONS.include?(operation[0])
        end
      end

      yield reply if reply
      reply
    end

    def ensure_connected
      tries = 0

      begin
        connect unless connected?

        tries += 1
        yield
      rescue ConnectionError
        disconnect

        if tries < 2
          retry
        else
          down!
          raise
        end
      rescue Exception
        disconnect
        raise
      end
    end

    def down!
      @down_at = Time.now
    end

    def down_at
      @down_at
    end

    def down?
      !!@down_at
    end

    def connection
      @connection ||= Connection.new
    end

    def connected?
      connection.connected?
    end

    def connect
      connection.connect host, port, timeout

      @down_at = nil
    rescue TimeoutError
      raise CannotConnectError, "Timed out connecting to Mongo on #{host}:#{port}"
    rescue Errno::ECONNREFUSED
      raise CannotConnectError, "Error connecting to Mongo on #{host}:#{port} (ECONNREFUSED)"
    end
  end
end
