require_relative "../test_helper"

module Unit
  class TestMonetDB < MiniTest::Test

    describe MonetDB do
      it "has the current version" do
        version = File.read(path("VERSION")).strip
        assert_equal version, MonetDB::VERSION
        assert File.read(path "CHANGELOG.rdoc").include?("Version #{version} ")
      end

      describe ".logger" do
        it "returns its instance variable :@logger" do
          MonetDB.instance_variable_set :@logger, (logger = mock)
          assert_equal logger, MonetDB.logger
        end
      end

      describe ".logger=" do
        it "stores the passed value as the instance variable :@logger" do
          MonetDB.logger = (logger = mock)
          assert_equal logger, MonetDB.instance_variable_get(:@logger)
        end
      end

      describe ".configurations" do
        it "returns its instance variable :@configurations" do
          MonetDB.instance_variable_set :@configurations, (configurations = mock)
          assert_equal configurations, MonetDB.configurations
        end
      end

      describe ".configurations=" do
        it "stores the passed hash as the instance variable :@configurations" do
          MonetDB.configurations = (configurations = {})
          assert_equal configurations, MonetDB.instance_variable_get(:@configurations)
        end

        it "stringifies the passed hash" do
          MonetDB.configurations = (configurations = {:a => "b"})
          assert_equal({"a" => "b"}, MonetDB.instance_variable_get(:@configurations))
        end
      end

      describe ".establish_connection" do
        describe "valid" do
          before do
            @connection = mock
            @connection.expects(:connect)
          end

          it "accepts configuration hashes" do
            config = {"host" => "localhost"}
            MonetDB::Connection.expects(:new).with(config).returns(@connection)
            MonetDB.establish_connection config
          end

          it "accepts configuration names" do
            config = {"host" => "localhost"}
            MonetDB.instance_variable_set(:@configurations, {"foo" => config})
            MonetDB::Connection.expects(:new).with(config).returns(@connection)
            MonetDB.establish_connection "foo"
          end
        end

        describe "invalid" do
          it "denies non-configuration arguments" do
            assert_raises MonetDB::ConnectionError do
              MonetDB.establish_connection 123
            end
            assert_raises MonetDB::ConnectionError do
              MonetDB.establish_connection true
            end
            assert_raises MonetDB::ConnectionError do
              MonetDB.establish_connection "foo"
            end
          end
        end
      end

      describe ".connection" do
        it "returns its instance variable :@connection" do
          MonetDB.instance_variable_set :@connection, (connection = mock)
          assert_equal connection, MonetDB.connection
        end
      end
    end

  end
end
