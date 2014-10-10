$:.unshift File.expand_path("../../lib", __FILE__)

require_relative "test_helper/coverage"

require "minitest/autorun"
require "mocha/setup"

def path(path)
  File.expand_path "../../#{path}", __FILE__
end

require "bundler"
Bundler.require :default, :development, :test

require_relative "test_helper/minitest"
