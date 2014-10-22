require_relative "../../test_helper"

module Unit
  module Connection
    class TestLogger < MiniTest::Test

      class Connection < SimpleConnection
        include MonetDB::Connection::Logger
      end

      describe MonetDB::Connection::Logger do
        before do
          @connection = Connection.new
        end

        describe "#log" do
          describe "when MonetDB.logger" do
            it "delegates to MonetDB.logger" do
              (logger = mock).expects(:info, "Hello world!")
              MonetDB.expects(:logger).returns(logger).twice
              @connection.send :log, :info, "Hello world!"
            end
          end

          describe "when no MonetDB.logger" do
            it "does nothing" do
              assert_nil @connection.send(:log, :info, "Boo!")
            end
          end
        end
      end

    end
  end
end
