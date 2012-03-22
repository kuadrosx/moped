class ReplicaSet
  def node_for(consistency)
    resync if nodes.any? { |n| n.down? && n.down_at < (Time.now - 30) }

    if consistency == :strong
      primary
    else
      secondaries.sample || primary
    end
  end

  def resync
    sync_node = ->(addr) do
      node = Node.new addr

      unless seen[node.resolved_address]
        seen[node.resolved_address] = true

        node.refresh!
        nodes << node

        node.known_hosts.each do |host|
          sync_node[host]
        end
      end
    end
  end

  def primary
    nodes.find(&:primary?) || raise "No primary node"
  end
end

class Session
  READ_OPERATIONS = [:query, :get_more]

  def perform(command, *args)
    read = READ_OPERATIONS.include?(command)
    consistency = read ? self.consistency : :strong

    node = cluster.node_for(consistency)

    if !read && safe
      return node.send_safely([command, *args], safety), node
    else
      return node.send(command, *args), node
    end
  end
end

class Database
  def command(command)
    result, = session.perform :command, command
  end
end

class Collection
  def insert(documents)
    documents = Array(documents)

    result, = session.perform :insert, database.name, name, documents
  end
end

class Cursor
  def more
    if loaded?
      result, @node = session.perform :query, query
    else
      @node.perform :get_more, cursor
    end
  end
end
