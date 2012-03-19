require "pty"
require "expect"

$expect_verbose = true

PTY.spawn "mongo --nodb replica_set.js --shell" do |reader, writer, pid|
  reader.expect /> /
  writer.puts "var rst = new ReplSetTest({ nodes : 3, startPort: 44000 })"
  reader.expect /> /
  writer.puts "rst.startSet()"
  reader.expect /> /
  writer.puts "rst.initiate()"
  reader.expect /> /
  writer.puts "rst.awaitReplication()"
  reader.expect /> /
end
