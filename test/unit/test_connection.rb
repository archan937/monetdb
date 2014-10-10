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

      describe "#set_timezone_interval" do
        describe "when not already set" do
          describe "when success" do
            it "returns true" do
              time = mock
              time.expects(:gmt_offset).returns(7200)
              Time.expects(:now).returns(time)

              @connection.expects(:write).with("sSET TIME ZONE INTERVAL '+02:00' HOUR TO MINUTE;")
              @connection.expects(:read).returns("")
              assert_equal true, @connection.send(:set_timezone_interval)

              @connection.instance_variable_set(:@timezone_interval_set, false)

              time = mock
              time.expects(:gmt_offset).returns(36000)
              Time.expects(:now).returns(time)

              @connection.expects(:write).with("sSET TIME ZONE INTERVAL '+10:00' HOUR TO MINUTE;")
              @connection.expects(:read).returns("")
              assert_equal true, @connection.send(:set_timezone_interval)
            end
          end

          describe "when fail" do
            it "raises a command error" do
              @connection.expects(:write)
              @connection.expects(:read).returns("!epicfail")
              assert_raises MonetDB::CommandError do
                @connection.send(:set_timezone_interval)
              end
            end
          end
        end

        describe "when already set" do
          it "returns false" do
            @connection.instance_variable_set(:@timezone_interval_set, true)
            assert_equal false, @connection.send(:set_timezone_interval)
          end
        end
      end

      describe "#set_reply_size" do
        describe "when not already set" do
          describe "when success" do
            it "returns true" do
              @connection.expects(:write).with("Xreply_size #{MonetDB::Connection::REPLY_SIZE}\n")
              @connection.expects(:read).returns("")
              assert_equal true, @connection.send(:set_reply_size)
            end
          end

          describe "when fail" do
            it "raises a command error" do
              @connection.expects(:write)
              @connection.expects(:read).returns("!epicfail")
              assert_raises MonetDB::CommandError do
                @connection.send(:set_reply_size)
              end
            end
          end
        end

        describe "when already set" do
          it "returns false" do
            @connection.instance_variable_set(:@reply_size_set, true)
            assert_equal false, @connection.send(:set_reply_size)
          end
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

      describe "#write" do
        describe "when disconnected" do
          it "raises a connection error" do
            assert_raises MonetDB::ConnectionError do
              @connection.send(:write, "")
            end
          end
        end

        describe "when connected" do
          it "writes chunks to the active socket provided with the length header" do
            socket = mock
            socket.expects(:write).with("\f\x00Hello")
            socket.expects(:write).with("\t\x00World!")

            @connection.instance_variable_set(:@socket, socket)
            @connection.expects(:pack).with("foo bar").returns(["\f\x00Hello", "\t\x00World!"])

            assert_equal true, @connection.send(:write, "foo bar")
          end
        end
      end

      describe "#pack" do
        it "returns chunks provided with a length header" do
          message = "BIG:monetdb:{MD5}6432e841c9943d524b9b922ee1e5924a:sql:test_drive:"
          assert_equal ["\x83\x00#{message}"], @connection.send(:pack, message)

          message = "hKszBZEmQ1uOPYrpVFEc:merovingian:9:RIPEMD160,SHA256,SHA1,MD5:LIT:SHA512:"
          assert_equal ["\x91\x00#{message}"], @connection.send(:pack, message)

          message.expects(:scan).with(/.{1,#{MonetDB::Connection::MAX_MSG_SIZE}}/).returns(%w(foobar bazqux paul))
          assert_equal [
            "\f\x00foobar",
            "\f\x00bazqux",
            "\t\x00paul"
          ], @connection.send(:pack, message)
        end
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
