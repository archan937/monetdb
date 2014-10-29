require "monetdb/connection/query/table"

module MonetDB
  class Connection
    module Query

      def self.included(base)
        base.send :include, Table
      end

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

      def extract_headers!(response)
        [parse_query_header!(response), parse_scheme_header!(response)]
      end

      def parse_query_header!(response)
        header = response.shift

        raise QueryError, header if header[0].chr == MSG_ERROR

        unless header[0].chr == MSG_QUERY
          ENV["MONETDB_QUERY_RESPONSE"] = ([header] + response).join("\n").inspect
          disconnect if reconnect?
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

    end
  end
end
