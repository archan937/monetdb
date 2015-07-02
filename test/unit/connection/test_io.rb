#encoding: UTF-8

require_relative "../../test_helper"

module Unit
  module Connection
    class TestIO < MiniTest::Test

      class Connection < SimpleConnection
        include MonetDB::Connection::Messages
        include MonetDB::Connection::IO
      end

      describe MonetDB::Connection::IO do
        before do
          @connection = Connection.new
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

              first_chunk = " " * 44
              last_chunk = " " * 22

              socket.expects(:recv).with(2).returns("\x85\x00")
              socket.expects(:recv).with(66).returns(first_chunk)
              socket.expects(:recv).with(22).returns(last_chunk)

              assert_equal first_chunk + last_chunk, @connection.send(:read)
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
            assert_equal ["#{[131].pack("v").force_encoding('utf-8')}#{message}"], @connection.send(:pack, message)

            message = "hKszBZEmQ1uOPYrpVFEc:merovingian:9:RIPEMD160,SHA256,SHA1,MD5:LIT:SHA512:"
            assert_equal ["#{[145].pack("v").force_encoding('utf-8')}#{message}"], @connection.send(:pack, message)

            message = "Message with multibyte chars: ✷"
            assert_equal ["#{[67].pack("v").force_encoding('utf-8')}#{message}"], @connection.send(:pack, message)

            message.expects(:scan).with(/.{1,#{MonetDB::Connection::MAX_MSG_SIZE}}/m).returns(%w(foobar bazqux paul))
            assert_equal [
              "#{[12].pack("v")}foobar",
              "#{[12].pack("v")}bazqux",
              "#{[9].pack("v")}paul"
            ], @connection.send(:pack, message)
          end
        end
      end

    end
  end
end
