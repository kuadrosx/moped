require "spec_helper"
require "moped/cluster"

describe Moped::Cluster do

  let(:master) do
    TCPServer.new "127.0.0.1", 0
  end

  let(:secondary_1) do
    TCPServer.new "127.0.0.1", 0
  end

  let(:secondary_2) do
    TCPServer.new "127.0.0.1", 0
  end

  describe "connecting directly to the master node" do
  end

  describe "connecting directly to a slave node"

  describe "connecting with a single seed"
  describe "connecting with multiple seeds"

  describe "when secondary goes away"
  describe "when master goes away"
  describe "when only slaves are available"

end
