require_relative "../../test_helper"

module Unit
  module Connection
    class TestQuery < MiniTest::Test

      class Connection < SimpleConnection
        include MonetDB::Connection::Messages
        include MonetDB::Connection::Query
      end

    end
  end
end
