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
        describe "when supporting server specifications" do
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

        describe "when not supporting any of the server protocols" do
          it "raises a protocol error" do
            socket = mock
            @connection.instance_variable_set(:@socket, socket)

            response = "<salt>:<server_name>:0:MD5,SHA512:<server_endianness>:SHA1"
            socket.expects(:recv).with(2).returns("\x85\x00")
            socket.expects(:recv).with(66).returns(response)

            assert_raises MonetDB::ProtocolError do
              @connection.send(:obtain_server_challenge)
            end
          end
        end

        describe "when not supporting any of the server auth types" do
          it "raises an authentication error" do
            socket = mock
            @connection.instance_variable_set(:@socket, socket)

            response = "<salt>:<server_name>:9:CRYPT:<server_endianness>:SHA1"
            socket.expects(:recv).with(2).returns("\x85\x00")
            socket.expects(:recv).with(66).returns(response)

            assert_raises MonetDB::AuthenticationError do
              @connection.send(:obtain_server_challenge)
            end
          end
        end
      end

      describe "#authenticate" do
        before do
          @connection.expects(:obtain_server_challenge)
        end

        describe "with MAPI V8 protocol" do
          before do
            @connection.instance_variable_get(:@config)[:protocol] = MonetDB::Connection::MAPI_V8
            @connection.expects(:mapi_v8_auth_string).returns(auth_string = "<v8_auth_string>")
            @connection.expects(:write).with(auth_string)
          end

          describe "when success" do
            it "returns true" do
              @connection.expects(:read).returns("")
              @connection.send(:authenticate)
            end
          end

          describe "when redirect" do
            it "authenticates using the redirect" do
              @connection.expects(:read).returns(response = "^^mapi:redirect")
              @connection.expects(:authentication_redirect).with(response)
              @connection.send(:authenticate)
            end
          end

          describe "when fail" do
            it "raises an authentication error" do
              @connection.expects(:read).returns("!epicfail")
              assert_raises MonetDB::AuthenticationError do
                @connection.send(:authenticate)
              end
            end
          end
        end

        describe "with MAPI V9 protocol" do
          before do
            @connection.instance_variable_get(:@config)[:protocol] = MonetDB::Connection::MAPI_V9
            @connection.expects(:mapi_v9_auth_string).returns(auth_string = "<v9_auth_string>")
            @connection.expects(:write).with(auth_string)
          end

          describe "when success" do
            it "returns true" do
              @connection.expects(:read).returns("")
              @connection.send(:authenticate)
            end
          end

          describe "when redirect" do
            it "authenticates using the redirect" do
              @connection.expects(:read).returns(response = "^^mapi:redirect")
              @connection.expects(:authentication_redirect).with(response)
              @connection.send(:authenticate)
            end
          end

          describe "when fail" do
            it "raises an authentication error" do
              @connection.expects(:read).returns("!epicfail")
              assert_raises MonetDB::AuthenticationError do
                @connection.send(:authenticate)
              end
            end
          end
        end
      end

      describe "authentication strings" do
        before do
          @endianness = MonetDB::Connection::ENDIANNESS
          @lang = MonetDB::Connection::LANG
          @connection.instance_variable_set :@authentication_redirects, 0
          @connection.instance_variable_set :@config, {
            :database => "db",
            :username => "paul",
            :password => "mysecret",
            :salt => "kitchen",
            :password_digest_method => "MD5"
          }
        end

        describe "#authentication_redirect" do
          describe "without a MAPI redirect" do
            it "raises an authentication error" do
              assert_raises MonetDB::AuthenticationError do
                @connection.send(:authentication_redirect, "^foobar:bazqux")
              end
            end
          end

          describe "without a valid redirect URI" do
            it "raises an authentication error" do
              URI.expects(:split).raises(URI::InvalidURIError)
              assert_raises MonetDB::AuthenticationError do
                @connection.send(:authentication_redirect, "^mapi:foobar")
              end
            end
          end

          describe "with 'merovingian' scheme" do
            it "retries to authenticate" do
              @connection.expects(:authenticate)
              @connection.send(:authentication_redirect, "^mapi:merovingian://foobar")
            end

            describe "when too many retries" do
              it "raises an authentication error" do
                @connection.expects(:obtain_server_challenge).times(5)
                @connection.expects(:write).times(5)
                @connection.expects(:read).times(5).returns(response = "^mapi:merovingian://foobar")
                assert_raises MonetDB::AuthenticationError do
                  @connection.send(:authentication_redirect, response)
                end
              end
            end
          end

          describe "with 'monetdb' scheme" do
            it "raises an authentication error" do
              @connection.expects(:connect)
              @connection.send(:authentication_redirect, "^mapi:monetdb://ghost:1982")
              assert_equal "ghost", @connection.send(:config)[:host]
              assert_equal "1982", @connection.send(:config)[:port]
            end
          end

          describe "without a supported server name" do
            it "raises an authentication error" do
              assert_raises MonetDB::AuthenticationError do
                @connection.send(:authentication_redirect, "^mapi:foobar")
              end
            end
          end
        end

        describe "#mapi_v8_auth_string" do
          describe "when using digests" do
            it "returns an authentication string with a digest hashsum" do
              @connection.instance_variable_get(:@config)[:auth_type] = "MD5"
              hashsum = "ec2c417dbdaa6fec77eef74e8c2524b9"
              assert_equal "#{@endianness}:paul:{MD5}#{hashsum}:#{@lang}:db:", @connection.send(:mapi_v8_auth_string)

              @connection.instance_variable_get(:@config)[:auth_type] = "SHA1"
              hashsum = "10cbdc8ea82154bbfe64df5265a2f35118b51a18"
              assert_equal "#{@endianness}:paul:{SHA1}#{hashsum}:#{@lang}:db:", @connection.send(:mapi_v8_auth_string)

              @connection.instance_variable_get(:@config)[:auth_type] = "SHA512"
              hashsum = "8527a8feaa8ea3e538b16a1a40e76176edb725b29bb8989ec4800de625f6b1d3569eb7fae49b0b54ae33bbf055e462a769582d0f02a18181e5dac1b106710d98"
              assert_equal "#{@endianness}:paul:{SHA512}#{hashsum}:#{@lang}:db:", @connection.send(:mapi_v8_auth_string)
            end
          end

          describe "when plain authentication type" do
            it "returns an authentication string with a plain hashsum" do
              @connection.instance_variable_get(:@config)[:auth_type] = "PLAIN"
              assert_equal "#{@endianness}:paul:{PLAIN}mysecretkitchen:#{@lang}:db:", @connection.send(:mapi_v8_auth_string)
            end
          end

          describe "when non-supported authentication type" do
            it "returns an authentication string without hashsum" do
              assert_equal "#{@endianness}:paul:{}:#{@lang}:db:", @connection.send(:mapi_v8_auth_string)
            end
          end
        end

        describe "#mapi_v9_auth_string" do
          before do
            @connection.instance_variable_get(:@config)[:password_digest_method] = "MD5"
          end

          describe "when using digests" do
            it "returns an authentication string with a digest hashsum" do
              @connection.instance_variable_get(:@config)[:auth_type] = "MD5"
              hashsum = "2c89f883c0a9e30f58585f15adeb0500"
              assert_equal "#{@endianness}:paul:{MD5}#{hashsum}:#{@lang}:db:", @connection.send(:mapi_v9_auth_string)

              @connection.instance_variable_get(:@config)[:auth_type] = "SHA1"
              hashsum = "51f0d237aedd38ad10f3348ecaa95d1d9fc7d7fd"
              assert_equal "#{@endianness}:paul:{SHA1}#{hashsum}:#{@lang}:db:", @connection.send(:mapi_v9_auth_string)

              @connection.instance_variable_get(:@config)[:auth_type] = "SHA512"
              hashsum = "cf623a200ef12a7413835745bd0287cba478f1eb275c57137f7bdcea20ec10897544ad7a45647c3b814ff9e9e2fb187e6277fe2f994b23bb7d09f2141f83fffa"
              assert_equal "#{@endianness}:paul:{SHA512}#{hashsum}:#{@lang}:db:", @connection.send(:mapi_v9_auth_string)
            end
          end

          describe "when plain authentication type" do
            it "returns an authentication string with a plain hashsum" do
              @connection.instance_variable_get(:@config)[:auth_type] = "PLAIN"
              assert_equal "#{@endianness}:paul:{PLAIN}mysecretkitchen:#{@lang}:db:", @connection.send(:mapi_v9_auth_string)
            end
          end

          describe "when non-supported authentication type" do
            it "returns an authentication string without hashsum" do
              assert_equal "#{@endianness}:paul:{}:#{@lang}:db:", @connection.send(:mapi_v9_auth_string)
            end
          end
        end
      end

      describe "#hexdigest" do
        it "returns the hexdigest using the passed method and value" do
          assert_equal "acbd18db4cc2f85cedef654fccc4a4d8", @connection.send(:hexdigest, "MD5", "foo")
          assert_equal "0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33", @connection.send(:hexdigest, "SHA1", "foo")
          assert_equal "f7fbba6e0636f890e56fbbf3283e524c6fa3204ae298382d624741d0dc6638326e282c41be5e4254d8820772c5518a2c5a8c0c7f7eda19594a7eb539453e1ed7", @connection.send(:hexdigest, "SHA512", "foo")
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
