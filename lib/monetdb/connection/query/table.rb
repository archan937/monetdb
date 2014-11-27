module MonetDB
  class Connection
    module Query
      module Table

        def parse_table_response(query_header, table_header, response)
          unless query_header[:rows] == response.size
            raise QueryError, "Amount of fetched rows does not match header value (#{response.size} instead of #{query_header[:rows]})"
          end
          parse_table_rows query_header, table_header, response.join("\n")
        end

        def parse_table_rows(query_header, table_header, response)
          start = Time.now
          rows = response.slice(0..-3).split("\t]\n")
          parse_rows(table_header, rows).tap do
            log :info, "  [1m[36mRUBY (#{((Time.now - start) * 1000).round(1)}ms)[0m [ Rows: #{query_header[:rows]}, Bytesize: #{response.bytesize} bytes ][0m"
          end
        end

        def parse_rows(table_header, rows)
          column_types = table_header[:column_types]
          rows.collect do |row|
            parse_row column_types, row
          end
        end

        def parse_row(column_types, row)
          [].tap do |parsed|
            row.slice(1..-1).split(",\t").each_with_index do |value, index|
              parsed << parse_value(column_types[index], value.strip)
            end
          end
        end

        def parse_value(type, value)
          unless value == "NULL"
            case type
            when :varchar, :text
              parse_string_value value
            when :int, :smallint, :bigint, :serial, :wrd
              parse_integer_value value
            when :double, :float, :real
              parse_float_value value
            when :date
              parse_date_value value
            when :timestamp
              parse_date_time_value value
            when :tinyint
              parse_boolean_value value
            else
              raise NotImplementedError, "Cannot parse value of type #{type.inspect}"
            end
          end
        end

        def parse_string_value(value)
          value.slice(1..-2).force_encoding("UTF-8")
        end

        def parse_integer_value(value)
          value.to_i
        end

        def parse_float_value(value)
          value.to_f
        end

        def parse_date_value(value)
          Date.new *value.split("-").collect(&:to_i)
        end

        def parse_date_time_value(value)
          date, time = value.split(" ")
          Time.new *(date.split("-") + time.split(":")).collect(&:to_i)
        end

        def parse_boolean_value(value)
          value == "1"
        end

      end
    end
  end
end
