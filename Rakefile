require "rake/testtask"
require "rspec/core/rake_task"

RSpec::Core::RakeTask.new(:spec) do |spec|
  spec.pattern = "spec/**/*_spec.rb"
end

Rake::TestTask.new do |t|
  t.test_files = FileList['socket_test.rb']
  t.verbose = true
end

task :default => :test
