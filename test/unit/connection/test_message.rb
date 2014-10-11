require_relative "../../test_helper"

module Unit
  module Connection
    class TestSetup < MiniTest::Test

      class Connection < SimpleConnection
        include MonetDB::Connection::Message
      end

      describe MonetDB::Connection::Message do
        before do
          @connection = Connection.new
        end

        describe "#msg_chr" do
          describe "when passing an empty string" do
            it "returns an empty string" do
              assert_equal "", @connection.send(:msg_chr, "")
            end
          end

          describe "when passing a non-empty string" do
            it "returns the first character" do
              assert_equal " ", @connection.send(:msg_chr, "   ")
              assert_equal "%", @connection.send(:msg_chr, "%foobar")
            end
          end
        end

        describe "#msg?" do
          it "verifies whether the passed string matches the passed message character" do
            assert_equal true , @connection.send(:msg?, "!syntax error", MonetDB::Connection::MSG_ERROR)
            assert_equal false, @connection.send(:msg?, "!syntax error", MonetDB::Connection::MSG_PROMPT)

            assert_equal true , @connection.send(:msg?, "", MonetDB::Connection::MSG_PROMPT)
            assert_equal false, @connection.send(:msg?, "", MonetDB::Connection::MSG_ERROR)

            @connection.expects(:msg_chr).with("foo").twice.returns("!")
            assert_equal true , @connection.send(:msg?, "foo", MonetDB::Connection::MSG_ERROR)
            assert_equal false, @connection.send(:msg?, "foo", MonetDB::Connection::MSG_PROMPT)
          end
        end
      end

    end
  end
end
