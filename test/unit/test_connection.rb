require_relative "../test_helper"

module Unit
  class TestConnection < MiniTest::Test

    describe MonetDB::Connection do
      before do
        @connection = MonetDB::Connection.new
      end

      describe "#initialize" do
        describe "when not passing a configuration" do
          it "uses a default configuration" do
            assert_equal({
              :host => "localhost",
              :port => 50000,
              :username => "monetdb",
              :password => "monetdb"
            }, @connection.instance_variable_get(:@config))
          end
        end

        describe "when passing a configuration" do
          it "overrides the default configuration" do
            connection = MonetDB::Connection.new "host" => "127.0.0.1"
            assert_equal({
              :host => "127.0.0.1",
              :port => 50000,
              :username => "monetdb",
              :password => "monetdb"
            }, connection.instance_variable_get(:@config))
          end
        end
      end

      describe "#connect" do
        it "defines an active socket" do
          TCPSocket.expects(:new).returns(socket = mock)

          assert_nil @connection.instance_variable_get(:@socket)

          @connection.expects(:setup)
          @connection.connect
          assert_equal socket, @connection.instance_variable_get(:@socket)
        end
      end

      describe "#disconnect" do
        describe "when disconnected" do
          it "does nothing" do
            assert_equal false, @connection.connected?
            @connection.disconnect
          end
        end

        describe "when connected" do
          it "disconnects the socket and releases @socket" do
            socket = mock
            @connection.instance_variable_set(:@socket, socket)

            assert_equal true, @connection.connected?
            socket.expects(:disconnect)

            @connection.disconnect

            assert_equal nil, @connection.instance_variable_get(:@socket)
            assert_equal false, @connection.connected?
          end
        end
      end

      describe "#connected?" do
        it "returns whether it has an active socket" do
          assert_equal false, @connection.connected?
          @connection.instance_variable_set :@socket, mock
          assert_equal true, @connection.connected?
          @connection.instance_variable_set :@socket, nil
          assert_equal false, @connection.connected?
        end
      end

      describe "#reconnect?" do
        describe "at default" do
          it "returns false" do
            assert_equal false, @connection.reconnect?
          end
        end

        describe "when configured to true" do
          it "returns true" do
            @connection.instance_variable_get(:@config)[:reconnect] = true
            assert_equal true, @connection.reconnect?
          end
        end
      end

      describe "#check_connectivity!" do
        describe "when configured reconnect: true" do
          before do
            @connection.expects(:reconnect?).returns(true)
          end

          describe "when disconnected" do
            it "connects" do
              @connection.expects(:connected?).returns(false)
              @connection.expects(:connect)
              @connection.check_connectivity!
            end
          end

          describe "when connected" do
            it "does nothing" do
              @connection.expects(:connected?).returns(true)
              @connection.expects(:connect).never
              @connection.check_connectivity!
            end
          end
        end

        describe "when not configured reconnect: true" do
          describe "when disconnected" do
            it "does nothing" do
              @connection.expects(:connect).never
              @connection.check_connectivity!
            end
          end

          describe "when connected" do
            it "does nothing" do
              @connection.expects(:connect).never
              @connection.check_connectivity!
            end
          end
        end
      end

      describe "#socket" do
        it "returns its instance variable :@socket" do
          @connection.instance_variable_set :@socket, (socket = mock)
          assert_equal socket, @connection.send(:socket)
        end
      end

      describe "#log" do
        describe "without defined MonetDB.logger" do
          it "does nothing" do
            @connection.send(:log, :info, "This is a log line!")
          end
        end

        describe "with defined MonetDB.logger" do
          it "delegates to MonetDB.logger" do
            logger = mock
            MonetDB.instance_variable_set :@logger, logger

            logger.expects(:info).with("Testing!")
            @connection.send(:log, :info, "Testing!")

            logger.expects(:error).with("Boom!")
            @connection.send(:log, :error, "Boom!")
          end
        end
      end
    end

  end
end
