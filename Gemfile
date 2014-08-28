source "https://rubygems.org"

gemspec

group :development do
  gem "yard"
end

group :development, :test do
  gem "monetdb", :path => "."
  gem "pry"
end

group :test do
  gem "simplecov", :require => false
  gem "minitest"
  gem "mocha"
end
