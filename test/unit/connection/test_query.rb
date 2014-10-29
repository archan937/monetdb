require_relative "../../test_helper"

module Unit
  module Connection
    class TestQuery < MiniTest::Test

      class Connection < SimpleConnection
        include MonetDB::Connection::Messages
        include MonetDB::Connection::Query
        include MonetDB::Connection::Logger
      end

      describe MonetDB::Connection::Query do
        before do
          @connection = Connection.new
        end

        describe "#query" do
          describe "when disconnected" do
            it "raises a connection error" do
              assert_raises MonetDB::ConnectionError do
                @connection.query("")
              end
            end
          end

          describe "when connected" do
            before do
              @connection.instance_variable_set(:@socket, mock)
            end

            describe "when doing a select query" do
              describe "when returning the correct amount of records" do
                it "returns the result set" do
                  query = "SELECT * FROM foo_bars"
                  response = <<-RESPONSE
&1 0 2 2 2
% sys.foo_bars,\tsys.foo_bars # table_name
% id,\tname # name
% int,\tvarchar # type
% 2,\t17 # length
[ 1,\t"Paul Engel"\t]
[ 2,\t"Ken Adams"\t]
                  RESPONSE

                  @connection.expects(:write).with("s#{query};")
                  @connection.expects(:read).returns(response.strip)

                  assert_equal [
                    [1, "Paul Engel"],
                    [2, "Ken Adams"]
                  ], @connection.query(query)
                end
              end

              describe "when not returning the correct amount of records" do
                it "raises a query error" do
                  query = "SELECT * FROM foo_bars"
                  response = <<-RESPONSE
&1 0 3 2 2
% sys.foo_bars,\tsys.foo_bars # table_name
% id,\tname # name
% int,\tvarchar # type
% 2,\t17 # length
[ 1,\t"Paul Engel"\t]
[ 2,\t"Ken Adams"\t]
                  RESPONSE

                  @connection.expects(:write).with("s#{query};")
                  @connection.expects(:read).returns(response.strip)

                  assert_raises MonetDB::QueryError do
                    @connection.query(query)
                  end
                end
              end
            end

            describe "when doing any another type of query" do
              it "returns true" do
                query = "UPDATE foo_bars SET updated_at = NOW()"

                @connection.expects(:write).with("s#{query};")
                @connection.expects(:read).returns("&2 23 -1")

                assert_equal true, @connection.query(query)
              end
            end
          end
        end

        describe "#select_values" do
          before do
            @query = "SELECT id FROM relations ORDER BY id LIMIT 4"
          end
          describe "when empty result set" do
            it "returns an empty array" do
              @connection.expects(:select_rows).with(@query).returns([])
              assert_equal [], @connection.select_values(@query)
            end
          end
          describe "when getting data" do
            it "returns every first value of every row" do
              @connection.expects(:select_rows).with(@query).returns([[1934], [1947], [1980], [1982]])
              assert_equal [1934, 1947, 1980, 1982], @connection.select_values(@query)
            end
          end
        end

        describe "#select_value" do
          before do
            @query = "SELECT id FROM relations ORDER BY id LIMIT 1"
          end
          describe "when empty result set" do
            it "returns nil" do
              @connection.expects(:select_rows).with(@query).returns([])
              assert_nil @connection.select_value(@query)
            end
          end
          describe "when getting data" do
            it "returns the first value of the first row" do
              @connection.expects(:select_rows).with(@query).returns([[1982]])
              assert_equal 1982, @connection.select_value(@query)
            end
          end
        end

        describe "#extract_headers!" do
          it "returns an array with delegated return values" do
            response, foo, bar = mock, mock, mock

            @connection.expects(:parse_query_header!).with(response).returns(foo)
            @connection.expects(:parse_scheme_header!).with(response).returns(bar)

            assert_equal [foo, bar], @connection.send(:extract_headers!, response)
          end
        end

        describe "#parse_query_header!" do
          describe "containing an error message" do
            it "raises a query error" do
              assert_raises MonetDB::QueryError do
                @connection.send(:parse_query_header!, ["!epicfail", "foo"])
              end
            end
          end

          describe "when absent in response" do
            it "raises a query error" do
              assert_raises MonetDB::QueryError do
                @connection.send(:parse_query_header!, ["$money", "foo"])
              end
            end
          end

          describe "when present in response" do
            it "extracts the first line from the response and returns to_query_header_hash" do
              response = [
                "& 1 8 1982 0",
                "% foo",
                "% bar",
                "[ 19, 82 ]",
                "[ 20, 47 ]"
              ]

              @connection.expects(:to_query_header_hash).returns(hash = "hash")
              assert_equal hash, @connection.send(:parse_query_header!, response)
              assert_equal [
                "% foo",
                "% bar",
                "[ 19, 82 ]",
                "[ 20, 47 ]"
              ], response
            end
          end
        end

        describe "#to_query_header_hash" do
          describe "when Q_TABLE header" do
            it "returns a table header hash" do
              assert_equal({
                :type => "1",
                :id => 0,
                :rows => 1947,
                :columns => 8,
                :returned => 2014
              }, @connection.send(:to_query_header_hash, "&1 0 1947 8 2014"))
            end
          end
          describe "when Q_BLOCK header" do
            it "returns a block header hash" do
              assert_equal({
                :type => "6",
                :id => 0,
                :columns => 10,
                :remains => 179,
                :offset => 100
              }, @connection.send(:to_query_header_hash, "&6 0 10 179 100"))
            end
          end
          describe "when unknown header" do
            it "returns a simple hash with just the type" do
              assert_equal({:type => "0"}, @connection.send(:to_query_header_hash, "&0 1 2 3"))
            end
          end
        end

        describe "#parse_scheme_header!" do
          describe "when present in response" do
            it "extracts scheme related lines from the response and returns to_scheme_header_hash" do
              response = [
                "% foo",
                "% bar",
                "[ 19, 82 ]",
                "[ 20, 47 ]"
              ]

              @connection.expects(:to_scheme_header_hash).returns(hash = "hash")
              assert_equal hash, @connection.send(:parse_scheme_header!, response)
              assert_equal [
                "[ 19, 82 ]",
                "[ 20, 47 ]"
              ], response
            end
          end

          describe "when absent in response" do
            it "returns nil" do
              response = [
                "[ 19, 82 ]",
                "% foo bar",
                "[ 20, 47 ]"
              ]
              assert_nil @connection.send(:parse_scheme_header!, response)
              assert_equal [
                "[ 19, 82 ]",
                "% foo bar",
                "[ 20, 47 ]"
              ], response
            end
          end
        end

        describe "#to_scheme_header_hash" do
          it "returns a frozen hash" do
            header = [
              %w(foo_bars baz_quxs paul_engels),
              %w(foo bar baz qux),
              %w(tinyint varchar integer float),
              %w(1 9 8 2)
            ]

            hash = @connection.send(:to_scheme_header_hash, header)

            assert_equal true, hash.frozen?
            assert_equal({
              :table_name => "foo_bars",
              :column_names => %w(foo bar baz qux),
              :column_types => [:tinyint, :varchar, :integer, :float],
              :column_lengths => [1, 9, 8, 2]
            }, hash)
          end
        end

        describe "#parse_rows" do
          it "returns an array of hashes" do
            query_header = {
              :rows => 2
            }

            table_header = {
              :column_types => [:varchar, :date, :double]
            }

            response = [
              "[ \"Paul Engel\",\t1982-08-01,\t1709.34\t]",
              "[ \"Ken Adams\",\t1980-10-31,\t2003.47\t]"
            ].join("\n")

            assert_equal [
              ["Paul Engel", Date.parse("1982-08-01"), 1709.34],
              ["Ken Adams", Date.parse("1980-10-31"), 2003.47]
            ], @connection.send(:parse_table_rows, query_header, table_header, response)
          end
        end

        describe "#parse_value" do
          describe "when NULL" do
            it "returns nil" do
              assert_nil @connection.send(:parse_value, :varchar, "NULL")
              assert_nil @connection.send(:parse_value, :text, "NULL")
              assert_nil @connection.send(:parse_value, :int, "NULL")
              assert_nil @connection.send(:parse_value, :smallint, "NULL")
              assert_nil @connection.send(:parse_value, :bigint, "NULL")
              assert_nil @connection.send(:parse_value, :double, "NULL")
              assert_nil @connection.send(:parse_value, :float, "NULL")
              assert_nil @connection.send(:parse_value, :real, "NULL")
              assert_nil @connection.send(:parse_value, :date, "NULL")
              assert_nil @connection.send(:parse_value, :timestamp, "NULL")
              assert_nil @connection.send(:parse_value, :tinyint, "NULL")
            end
          end

          describe "when not NULL" do
            describe "when is type valid" do
              it "delegates to the appropiate parse method" do
                value = mock

                @connection.expects(:parse_string_value).with(value)
                @connection.send(:parse_value, :varchar, value)

                @connection.expects(:parse_string_value).with(value)
                @connection.send(:parse_value, :text, value)

                @connection.expects(:parse_integer_value).with(value)
                @connection.send(:parse_value, :int, value)

                @connection.expects(:parse_integer_value).with(value)
                @connection.send(:parse_value, :smallint, value)

                @connection.expects(:parse_integer_value).with(value)
                @connection.send(:parse_value, :bigint, value)

                @connection.expects(:parse_float_value).with(value)
                @connection.send(:parse_value, :float, value)

                @connection.expects(:parse_float_value).with(value)
                @connection.send(:parse_value, :real, value)

                @connection.expects(:parse_date_value).with(value)
                @connection.send(:parse_value, :date, value)

                @connection.expects(:parse_date_time_value).with(value)
                @connection.send(:parse_value, :timestamp, value)

                @connection.expects(:parse_boolean_value).with(value)
                @connection.send(:parse_value, :tinyint, value)
              end
            end

            describe "when is type invalid" do
              it "raises a not implemented error" do
                assert_raises NotImplementedError do
                  @connection.send(:parse_value, :foo, mock)
                end
              end
            end
          end
        end

        describe "#parse_string_value" do
          it "returns a string" do
            assert_equal "Paul Engel", @connection.send(:parse_string_value, "\"Paul Engel\"")
          end
        end

        describe "#parse_integer_value" do
          it "returns an integer" do
            assert_equal 1982, @connection.send(:parse_integer_value, "1982")
          end
        end

        describe "#parse_float_value" do
          it "returns a float" do
            assert_equal 19.82, @connection.send(:parse_float_value, "19.82")
          end
        end

        describe "#parse_date_value" do
          it "returns a Date instance" do
            assert_equal Date.parse("1982-08-01"), @connection.send(:parse_date_value, "1982-08-01")
          end
        end

        describe "#parse_date_time_value" do
          it "returns a Time instance" do
            assert_equal Time.parse("1982-08-01 18:19:47"), @connection.send(:parse_date_time_value, "1982-08-01 18:19:47.0000")
          end
        end

        describe "#parse_boolean_value" do
          it "returns a boolean" do
            assert_equal false, @connection.send(:parse_boolean_value, "0")
            assert_equal true, @connection.send(:parse_boolean_value, "1")
          end
        end
      end

    end
  end
end
