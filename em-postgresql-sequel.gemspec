# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'em-postgresql-sequel/version'

Gem::Specification.new do |gem|
  gem.name          = "em-postgresql-sequel"
  gem.version       = Em::Postgresql::Sequel::VERSION
  gem.authors       = ["Jan Zimmek, Asher Van Brunt"]
  gem.email         = ["jan.zimmek@web.de, asher@okbreathe.com"]
  gem.description   = %q{glue together eventmachine, postgresql and sequel}
  gem.summary       = %q{glue together eventmachine, postgresql and sequel}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_dependency "eventmachine"
  gem.add_dependency "pg"
  gem.add_dependency "sequel"

  gem.add_development_dependency "rspec"
end
