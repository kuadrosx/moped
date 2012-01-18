def sync_server(server)
  [].tap do |hosts|
    raise 
  end
ensure
  puts "ERROR IN ENSURE" if $!
end

sync_server "a"
