describe "Interacting with a healthy replica set" do

  specify "connecting to a single node should also find other nodes"
  specify "executing a write should always go to the primary"
  specify "executing a read should go to a secondary when the consistency level is eventual"
  specify "executing a read should go to the primary when the consistency level is strong"
  specify "getting more records from a cursor should always talk to the same as the original read"

  context "when a connection is dropped because of a network error" do
    it "tries to reconnect"

    context "after multiple failures" do
      it "marks the node as temporarily down"
    end
  end

end

describe "Interacting with a replica set with no nodes" do
  specify "everything throws an error"

  context "when a node becomes available" do
    specify "shit starts working again"
  end
end

describe "Interacting with a replica set with no master" do

  specify "it connects to all available nodes"
  specify "reads with eventual consistency work"
  specify "reads with strong consistency raise an error"
  specify "writes raise an error"

  context "when a master is elected" do
    specify "writes start working"
    specify "reads with strong consistency start working"
  end

end

describe "Interacting with a replica set with down secondary servers" do

  specify "it connects to all available nodes"
  specify "reads with eventual consistency work"
  specify "reads with strong consistency raise an error"
  specify "writes work"

  context "when a secondary server comes back up" do
    it "eventually directs reads there"
  end

end

describe "Interacting with a replica set when the master goes down" do

  specify "strongly consistent reads stop working"
  specify "writes stop working"

  context "if a new master is elected" do
    specify "writes start working"
    specify "reads with strong consistency start working"
  end

  context "if the original master comes back up" do
    specify "writes start working"
    specify "reads with strong consistency start working"
  end

end

describe "Interacting with a replica set when a secondary goes down" do

  specify "eventually consistent continue to work"
  specify "strongly consistent continue to work"
  specify "writes continue to work"

  context "when the original secondary comes back up" do
    it "eventually starts reading from it again"
  end

end
