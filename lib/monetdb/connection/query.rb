module MonetDB
  class Connection
    module Query

      def query(statement)
        check_connectivity!
        raise ConnectionError, "Not connected to server" unless connected?

        start = Time.now

        write "s#{statement};"
        response = read.split("\n")

        log :info, "\n  [1m[35mSQL (#{((Time.now - start) * 1000).round(1)}ms)[0m  #{statement}[0m"
        parse_response response
      end

      alias :select_rows :query

      def select_values(query)
        select_rows(query).collect{|x| x[0]}
      end

      def select_value(query)
        row = select_rows(query)[0]
        row[0] if row
      end

    private

      def parse_response(response)
        query_header, table_header = extract_headers!(response)
        if query_header[:type] == Q_TABLE
          parse_table_response query_header, table_header, response
        else
          true
        end
      end

      def parse_table_response(query_header, table_header, response)
        unless query_header[:rows] == response.size
          disconnect if reconnect?
          raise QueryError, "Amount of fetched rows does not match header value (#{response.size} instead of #{query_header[:rows]})"
        end
        parse_table_rows query_header, table_header, response.join("\n")
      end

      def extract_headers!(response)
        [parse_query_header!(response), parse_scheme_header!(response)]
      end

      def parse_query_header!(response)
        header = response.shift

        raise QueryError, header if header[0].chr == MSG_ERROR

        unless header[0].chr == MSG_QUERY
          ENV["MONETDB_QUERY_RESPONSE"] = ([header] + response).join("\n").inspect
          raise QueryError, "Expected an query header (#{MSG_QUERY}) but got (#{header[0].chr})"
        end

        to_query_header_hash header
      end

      def to_query_header_hash(header)
        hash = {:type => header[1].chr}

        keys = {
          Q_TABLE => [:id, :rows, :columns, :returned],
          Q_BLOCK => [:id, :columns, :remains, :offset]
        }[hash[:type]]

        if keys
          values = header.split(" ")[1, 4].collect(&:to_i)
          hash.merge! Hash[keys.zip(values)]
        end

        hash.freeze
      end

      def parse_scheme_header!(response)
        if (count = response.take_while{|x| x[0].chr == MSG_SCHEME}.size) > 0
          header = response.shift(count).collect{|x| x.gsub(/(^#{MSG_SCHEME}\s+|\s+#[^#]+$)/, "").split(/,?\s+/)}
          to_scheme_header_hash header
        end
      end

      def to_scheme_header_hash(header)
        table_name = header[0][0]
        column_names = header[1]
        column_types = header[2].collect(&:to_sym)
        column_lengths = header[3].collect(&:to_i)

        {:table_name => table_name, :column_names => column_names, :column_types => column_types, :column_lengths => column_lengths}.freeze
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
          when :int, :smallint, :bigint
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
