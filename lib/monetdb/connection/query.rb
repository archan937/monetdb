module MonetDB
  class Connection
    module Query

      def query(statement)
        raise ConnectionError, "Not connected to server" unless connected?

        write "s#{statement};"
        response = read.split("\n")

        query_header, table_header = extract_headers!(response)

        if query_header[:type] == Q_TABLE
          unless query_header[:rows] == response.size
            raise QueryError, "Amount of fetched rows does not match header value (#{response.size} instead of #{query_header[:rows]})"
          end
          response = parse_rows(table_header, response.join("\n"))
        else
          response = true
        end

        response
      end

      alias :select_rows :query

      def extract_headers!(response)
        [parse_query_header!(response), parse_scheme_header!(response)]
      end

      def parse_query_header!(response)
        header = response.shift

        raise QueryError, header if header[0].chr == MSG_ERROR

        unless header[0].chr == MSG_QUERY
          raise QueryError, "Expected an query header (#{MSG_QUERY}) but got (#{header[0].chr})"
        end

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

          table_name = header[0][0]
          column_names = header[1]
          column_types = Hash[column_names.zip(header[2].collect(&:to_sym))]
          column_lengths = header[3].collect(&:to_i)

          {:table_name => table_name, :column_names => column_names, :column_types => column_types, :column_lengths => column_lengths}.freeze
        end
      end

      def parse_rows(table_header, response)
        column_types = table_header[:column_types]
        types = table_header[:column_names].collect{|x| column_types[x]}

        response.split("\t]\n").collect do |row|
          parsed, values = [], row.slice(1..-1).split(",\t")
          values.each_with_index do |value, index|
            parsed << parse_value(types[index], value)
          end
          parsed
        end
      end

      def parse_value(type, value)
        unless value == "NULL"
          case type
          when :varchar, :text
            value.slice(1..-2).force_encoding("UTF-8")
          when :int, :smallint, :bigint
            value.to_i
          when :double, :float, :real
            value.to_f
          when :date
            Date.new *value.split("-").collect(&:to_i)
          when :timestamp
            date, time = value.split(" ")
            Time.new *(date.split("-") + time.split(":")).collect(&:to_i)
          when :tinyint
            value == "1"
          else
            raise NotImplementedError, "Cannot parse value of type #{type}"
          end
        end
      end

    end
  end
end
