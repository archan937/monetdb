require_relative "../../test_helper"

module Unit
  module Connection
    class TestAuthentication < MiniTest::Test

      class SimpleConnection
        include MonetDB::Connection::Authentication
        attr_reader :config
        def initialize
          @config = {
            :host => "localhost",
            :port => 50000,
            :username => "monetdb",
            :password => "monetdb"
          }
        end
        def msg_chr(string)
          string.empty? ? "" : string[0].chr
        end
      end

      describe MonetDB::Connection::Authentication do
        before do
          @connection = SimpleConnection.new
        end

        describe "#authenticate" do
          before do
            @connection.expects(:obtain_server_challenge!)
            @connection.expects(:authentication_string).returns(auth_string = "<authentication_string>")
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

        describe "#obtain_server_challenge!" do
          it "updates the config and validates it" do
            @connection.expects(:server_challenge).returns(:foo => "bar", :baz => "qux")
            @connection.expects(:assert_supported_protocol!)
            @connection.expects(:select_supported_auth_type!)

            assert_equal({
              :host => "localhost",
              :port => 50000,
              :username => "monetdb",
              :password => "monetdb"
            }, @connection.instance_variable_get(:@config))

            @connection.send(:obtain_server_challenge!)

            assert_equal({
              :host => "localhost",
              :port => 50000,
              :username => "monetdb",
              :password => "monetdb",
              :foo => "bar",
              :baz => "qux"
            }, @connection.instance_variable_get(:@config))
          end
        end

        describe "#server_challenge" do
          it "returns the server challenge config" do
            response = "<salt>:<server_name>:9:MD5,SHA512:<server_endianness>:SHA1"
            @connection.expects(:read).returns(response)
            assert_equal({
              :salt => "<salt>",
              :server_name => "<server_name>",
              :protocol => "9",
              :auth_types => "MD5,SHA512",
              :server_endianness => "<server_endianness>",
              :password_digest_method => "SHA1"
            }, @connection.send(:server_challenge))
          end
        end

        describe "#assert_supported_protocol!" do
          describe "when supporting one of the server protocols" do
            it "returns nil" do
              @connection.instance_variable_get(:@config)[:protocol] = MonetDB::Connection::MAPI_V8
              assert_nil @connection.send(:assert_supported_protocol!)
            end
          end

          describe "when not supporting any of the server protocols" do
            it "raises a protocol error" do
              @connection.instance_variable_get(:@config)[:protocol] = "FOO"
              assert_raises MonetDB::ProtocolError do
                @connection.send(:assert_supported_protocol!)
              end
            end
          end
        end

        describe "#select_supported_auth_type!" do
          describe "when supporting one of the server authentication types" do
            it "selects a supported authentication type" do
              config = @connection.instance_variable_get(:@config)
              auth_md5 = MonetDB::Connection::AUTH_MD5

              config[:auth_types] = "FOO,#{auth_md5},BAR"
              assert_nil config[:auth_type]

              @connection.send(:select_supported_auth_type!)
              assert_equal auth_md5, config[:auth_type]
            end
          end

          describe "when not supporting any of the server authentication types" do
            it "raises an authentication error" do
              response = "<salt>:<server_name>:9:CRYPT:<server_endianness>:SHA1"
              @connection.expects(:read).returns(response)
              assert_raises MonetDB::AuthenticationError do
                @connection.send(:obtain_server_challenge!)
              end
            end
          end
        end

        describe "#authentication_string" do
          before do
            @endianness = MonetDB::Connection::ENDIANNESS
            @lang = MonetDB::Connection::LANG
            @connection.instance_variable_set :@config, {
              :database => "db",
              :username => "paul",
              :password => "mysecret",
              :salt => "kitchen",
              :password_digest_method => "MD5"
            }
          end

          describe "MAPI V8 protocol" do
            before do
              @connection.instance_variable_get(:@config)[:protocol] = MonetDB::Connection::MAPI_V8
            end

            describe "when using digests" do
              it "returns an authentication string with a digest hashsum" do
                @connection.instance_variable_get(:@config)[:auth_type] = "MD5"
                hashsum = "ec2c417dbdaa6fec77eef74e8c2524b9"
                assert_equal "#{@endianness}:paul:{MD5}#{hashsum}:#{@lang}:db:", @connection.send(:authentication_string)

                @connection.instance_variable_get(:@config)[:auth_type] = "SHA1"
                hashsum = "10cbdc8ea82154bbfe64df5265a2f35118b51a18"
                assert_equal "#{@endianness}:paul:{SHA1}#{hashsum}:#{@lang}:db:", @connection.send(:authentication_string)

                @connection.instance_variable_get(:@config)[:auth_type] = "SHA512"
                hashsum = "8527a8feaa8ea3e538b16a1a40e76176edb725b29bb8989ec4800de625f6b1d3569eb7fae49b0b54ae33bbf055e462a769582d0f02a18181e5dac1b106710d98"
                assert_equal "#{@endianness}:paul:{SHA512}#{hashsum}:#{@lang}:db:", @connection.send(:authentication_string)
              end
            end

            describe "when plain authentication type" do
              it "returns an authentication string with a plain hashsum" do
                @connection.instance_variable_get(:@config)[:auth_type] = "PLAIN"
                assert_equal "#{@endianness}:paul:{PLAIN}mysecretkitchen:#{@lang}:db:", @connection.send(:authentication_string)
              end
            end

            describe "when non-supported authentication type" do
              it "returns an authentication string without hashsum" do
                assert_equal "#{@endianness}:paul:{}:#{@lang}:db:", @connection.send(:authentication_string)
              end
            end
          end

          describe "MAPI V9 protocol" do
            before do
              @connection.instance_variable_get(:@config)[:protocol] = MonetDB::Connection::MAPI_V9
              @connection.instance_variable_get(:@config)[:password_digest_method] = "MD5"
            end

            describe "when using digests" do
              it "returns an authentication string with a digest hashsum" do
                @connection.instance_variable_get(:@config)[:auth_type] = "MD5"
                hashsum = "2c89f883c0a9e30f58585f15adeb0500"
                assert_equal "#{@endianness}:paul:{MD5}#{hashsum}:#{@lang}:db:", @connection.send(:authentication_string)

                @connection.instance_variable_get(:@config)[:auth_type] = "SHA1"
                hashsum = "51f0d237aedd38ad10f3348ecaa95d1d9fc7d7fd"
                assert_equal "#{@endianness}:paul:{SHA1}#{hashsum}:#{@lang}:db:", @connection.send(:authentication_string)

                @connection.instance_variable_get(:@config)[:auth_type] = "SHA512"
                hashsum = "cf623a200ef12a7413835745bd0287cba478f1eb275c57137f7bdcea20ec10897544ad7a45647c3b814ff9e9e2fb187e6277fe2f994b23bb7d09f2141f83fffa"
                assert_equal "#{@endianness}:paul:{SHA512}#{hashsum}:#{@lang}:db:", @connection.send(:authentication_string)
              end
            end

            describe "when plain authentication type" do
              it "returns an authentication string with a plain hashsum" do
                @connection.instance_variable_get(:@config)[:auth_type] = "PLAIN"
                assert_equal "#{@endianness}:paul:{PLAIN}mysecretkitchen:#{@lang}:db:", @connection.send(:authentication_string)
              end
            end

            describe "when non-supported authentication type" do
              it "returns an authentication string without hashsum" do
                assert_equal "#{@endianness}:paul:{}:#{@lang}:db:", @connection.send(:authentication_string)
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
                @connection.expects(:obtain_server_challenge!).times(5)
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
      end

    end
  end
end
