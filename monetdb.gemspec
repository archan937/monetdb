# -*- encoding: utf-8 -*-
require File.expand_path("../lib/monetdb/version", __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Paul Engel"]
  gem.email         = ["pm_engel@icloud.com"]
  gem.summary       = %q{A pure Ruby database driver for MonetDB (monetdb5-sql)}
  gem.description   = %q{A pure Ruby database driver for MonetDB (monetdb5-sql)}
  gem.homepage      = "https://github.com/archan937/monetdb"

  gem.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.files         = `git ls-files`.split("\n")
  gem.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.name          = "monetdb"
  gem.require_paths = ["lib"]
  gem.version       = MonetDB::VERSION

  gem.add_dependency "activesupport"

  gem.add_development_dependency "rake"
  gem.add_development_dependency "pry"
  gem.add_development_dependency "simplecov"
  gem.add_development_dependency "minitest"
  gem.add_development_dependency "mocha"
end
