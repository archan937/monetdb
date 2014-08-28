require_relative "../test_helper"

module Unit
  class TestMonetDB < MiniTest::Test

    describe MonetDB do
      it "has the current version" do
        version = File.read path("VERSION")
        assert_equal version, MonetDB::VERSION
        assert File.read(path "CHANGELOG.rdoc").include?("Version #{version}")
      end
    end

  end
end