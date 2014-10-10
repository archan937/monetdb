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

      describe "#connected?" do
        it "returns whether it has an active socket" do
          assert_equal false, @connection.connected?
          @connection.instance_variable_set :@socket, mock
          assert_equal true, @connection.connected?
          @connection.instance_variable_set :@socket, nil
          assert_equal false, @connection.connected?
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

      describe "#socket" do
        it "returns its instance variable :@socket" do
          @connection.instance_variable_set :@socket, (socket = mock)
          assert_equal socket, @connection.send(:socket)
        end
      end

      describe "#setup" do
        it "obtains the server challenge, sets up the timezone and reply size" do
          @connection.expects(:authenticate)
          @connection.expects(:set_timezone_interval)
          @connection.expects(:set_reply_size)
          @connection.send(:setup)
        end
      end

      describe "#obtain_server_challenge" do
        it "enriches the config with server connection related information" do
          assert_equal({
            :host => "localhost",
            :port => 50000,
            :username => "monetdb",
            :password => "monetdb"
          }, @connection.instance_variable_get(:@config))

          socket = mock
          @connection.instance_variable_set(:@socket, socket)

          response = "<salt>:<server_name>:9:MD5,SHA512:<server_endianness>:SHA1"
          socket.expects(:recv).with(2).returns("\x85\x00")
          socket.expects(:recv).with(66).returns(response)
          @connection.send(:obtain_server_challenge)

          assert_equal({
            :host => "localhost",
            :port => 50000,
            :username => "monetdb",
            :password => "monetdb",
            :salt => "<salt>",
            :server_name => "<server_name>",
            :protocol => "9",
            :auth_types => "MD5,SHA512",
            :auth_type => "MD5",
            :server_endianness => "<server_endianness>",
            :password_digest_method => "SHA1"
          }, @connection.instance_variable_get(:@config))
        end
      end

      describe "#set_timezone_interval" do
        it "foo" do
        end
      end

      describe "#set_reply_size" do
        it "foo" do
        end
      end

      describe "#read" do
        describe "when disconnected" do
          it "raises a connection error" do
            assert_raises MonetDB::ConnectionError do
              @connection.send(:read)
            end
          end
        end
        describe "when connected" do
          it "obtains the block size and reads the server response" do
            socket = mock
            @connection.instance_variable_set(:@socket, socket)

            response = mock
            socket.expects(:recv).with(2).returns("\x85\x00")
            socket.expects(:recv).with(66).returns(response)

            assert_equal response, @connection.send(:read)
          end
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
