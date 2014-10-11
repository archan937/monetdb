require_relative "../../test_helper"

module Unit
  module Connection
    class TestSelect < MiniTest::Test

      class Connection < SimpleConnection
        include MonetDB::Connection::Message
        include MonetDB::Connection::Select
      end

    end
  end
end
