require "monetdb/core_ext"
require "monetdb/hasher"
require "monetdb/connection"
require "monetdb/transaction"
require "monetdb/data"
require "monetdb/error"
require "monetdb/version"

# A typical sequence of events is as follows: Create a database instance (handle), invoke query using the database handle to send the statement to the server and get back a result set object.
#
# A result set object is an instance of the MonetDB::Data class and has methods for fetching rows, moving around in the result set, obtaining column metadata, and releasing the result set.
#
# Records can be returned as arrays and iterators over the set.
#
# A database handler (dbh) is an instance of the MonetDB class.
#
#
# = Connection management
#
#   connect       - establish a new connection
#                   * user      : username (default is monetdb)
#                   * passwd    : password (default is monetdb)
#                   * lang      : language (default is sql)
#                   * host      : server hostanme or ip  (default is localhost)
#                   * port      : server port (default is 50000)
#                   * db_name   : name of the database to connect to
#                   * auth_type : hashing function to use during authentication (default is SHA1)
#   connected?    - returns true if there is an active connection to a server, false otherwise
#   reconnect     - reconnect to a server
#   close         - terminate a connection
#   auto_commit?  - returns ture if the session is running in auto commit mode, false otherwise
#   auto_commit   - enable/disable auto commit mode
#   query         - fire a query
#
# Currently MAPI protocols 8 and 9 are supported.
#
#
# = Managing record sets
#
# A record set is represented as an instance of the MonetDB::Data class; the class provides methods to manage retrieved data.
#
# The following methods allow to iterate over data:
#
#   fetch          - iterates over the record set and retrieves on row at a time. Each row is returned as an array
#   fetch_hash     - iterates over columns (on cell at a time)
#   fetch_all_hash - returns record set entries hashed by column name orderd by column position
#
# To return the record set as an array (with each tuple stored as array of fields) the following method can be used:
#
#   fetch_all      - fetch all rows and store them
#
# Information about the retrieved record set can be obtained via the following methods:
#
#   num_rows       - returns the number of rows present in the record set
#   num_fields     - returns the number of fields (columns) that compose the schema
#   name_fields    - returns the (ordered) name of the schema's columns
#   type_fields    - returns the (ordered) types list of the schema's columns
#
# To release a record set MonetDB::Data#free can be used.
#
#
# = Type conversion
#
# A mapping between SQL and ruby type is supported. Each retrieved field can be converted to a ruby datatype via
# a getTYPE method.
#
# The currently supported cast methods are:
#
#   getInt        - convert to an integer value
#   getFloat      - convert to a floating point value
#   getString     - return a string representation of the value, with trailing and leading " characters removed
#   getBlob       - convert an SQL stored HEX string to its binary representation
#   getTime       - return a string representation of a TIME field
#   getDate       - return a string representation of a DATE field
#   getDateTime   - convert a TIMESTAMP field to a ruby Time object
#   getChar       - on Ruby >= 1.9, convert a CHAR field to char
#   getBool       - convert a BOOLEAN field to a ruby bool object. If the value of the field is unknown, nil is returned
#   getNull       - convert a NULL value to a nil object
#
#
# = Transactions
#
# By default MonetDB works in auto_commit mode. To turn this feature off MonetDB#auto_commit(flag = false) can be used.
#
# Once auto_commit has been disabled it is possible to start transactions, create/delete savepoints, rollback and commit with
# the usual SQL statements.
#
# Savepoints IDs can be generated using the MonetDB#save method. To release a savepoint ID use MonetDB#release.
#
# You can access savepoints (as a stack) with the MonetDB#transactions method.
#

class MonetDB

  Q_TABLE             = "1" # SELECT operation
  Q_UPDATE            = "2" # INSERT/UPDATE operations
  Q_CREATE            = "3" # CREATE/DROP TABLE operations
  Q_TRANSACTION       = "4" # TRANSACTION
  Q_PREPARE           = "5" # QPREPARE message
  Q_BLOCK             = "6" # QBLOCK message

  MSG_REDIRECT        = "^" # auth redirection through merovingian
  MSG_QUERY           = "&"
  MSG_SCHEMA_HEADER   = "%"
  MSG_INFO            = "!" # info response from mserver
  MSG_TUPLE           = "["
  MSG_PROMPT          = ""

  REPLY_SIZE          = "-1"
  MAX_AUTH_ITERATION  = 10  # maximum number of auth iterations (through merovingian) allowed
  MONET_ERROR         = -1

  LANG_SQL            = "sql"
  LANG_XQUERY         = "xquery"
  XQUERY_OUTPUT_SEQ   = true # use MonetDB XQuery's output seq

  def self.logger=(logger)
    @logger = logger
  end

  def self.logger
    @logger
  end

  def self.configurations=(configurations)
    @configurations = configurations.inject({}){|h, (k, v)| h[k.to_s] = v; h}
  end

  def self.configurations
    @configurations
  end

  def self.establish_connection(arg)
    config = arg.is_a?(Hash) ? arg : (configurations || {})[arg.to_s]
    if config
      @connection = MonetDB.new.tap do |connection|
        config = {language: "sql", encryption: "SHA1"}.merge(config.inject({}){|h, (k, v)| h[k.to_sym] = v; h})
        connection.instance_variable_set(:@config, config)
        connection.connect *config.values_at(:username, :password, :language, :host, :port, :database, :encryption)
      end
    else
      raise Error, "Unable to establish connection for #{arg.inspect}"
    end
  end

  def self.connection
    @connection
  end

  # Establish a new connection.
  #   * user      : username (default is monetdb)
  #   * passwd    : password (default is monetdb)
  #   * lang      : language (default is sql)
  #   * host      : server hostanme or ip  (default is localhost)
  #   * port      : server port (default is 50000)
  #   * db_name   : name of the database to connect to
  #   * auth_type : hashing function to use during authentication (default is SHA1)
  def connect(username = "monetdb", password = "monetdb", lang = "sql", host = "127.0.0.1", port = "50000", db_name = "test", auth_type = "SHA1")
    # TODO: Handle pools of connections
    @username = username
    @password = password
    @lang = lang
    @host = host
    @port = port
    @db_name = db_name
    @auth_type = auth_type
    @connection = MonetDB::Connection.new(user = @username, passwd = @password, lang = @lang, host = @host, port = @port)
    @connection.connect(@db_name, @auth_type)
  end

  # Send a <b>user submitted</b> query to the server and store the response.
  #
  # Returns an instance of MonetDB::Data.
  def query(q = "")
    unless @connection.nil?
      @data = MonetDB::Data.new(@connection)
      @data.execute(q)
    end
    @data
  end

  # Send a <b>user submitted select</b> query to the server and store the response.
  #
  # Returns an array of arrays with casted values.
  def select_rows(qry)
    return if @connection.nil?
    data = MonetDB::Data.new @connection

    start = Time.now
    @connection.send(data.send(:format_query, qry))
    response = @connection.receive.split("\n")

    if (row = response[0]).chr == MSG_INFO
      raise QueryError, row
    end

    headers, rows = response.partition{|x| [MSG_SCHEMA_HEADER, MSG_QUERY].include? x[0]}
    data.send :receive_record_set, headers.join("\n")
    query_header = data.send :parse_header_query, headers.shift
    table_header = data.send :parse_header_table, headers
    rows = rows.join("\n")

    row_count = rows.split("\t]\n").size
    while row_count < query_header["rows"].to_i
      received = @connection.receive
      row_count += received.scan("\t]\n").size
      rows << received
    end
    rows = rows.split("\t]\n")

    log :info, "\n  [1m[35mSQL (#{((Time.now - start) * 1000).round(1)}ms)[0m  #{qry}[0m"

    column_types = table_header["columns_type"]
    types = table_header["columns_name"].collect{|x| column_types[x]}

    rows.collect do |row|
      parsed, values = [], row.gsub(/^\[\s*/, "").split(",\t")
      values.each_with_index do |value, index|
        parsed << begin
          unless value.strip == "NULL"
            case types[index]
            when "bigint", "int"
              value.to_i
            when "double"
              value.to_f
            else
              value.strip.gsub(/(^"|"$|\\|\")/, "").force_encoding("UTF-8")
            end
          end
        end
      end
      parsed
    end
  end

  # Returns whether a "connection" object exists.
  def connected?
    !@connection.nil?
  end

  # Reconnect to the server.
  def reconnect
    if @connection != nil
      self.close
      @connection = MonetDB::Connection.new(user = @username, passwd = @password, lang = @lang, host = @host, port = @port)
      @connection.connect(db_name = @db_name, auth_type = @auth_type)
    end
  end

  # Turn auto commit on/off.
  def auto_commit(flag = true)
    @connection.set_auto_commit(flag)
  end

  # Returns the current auto commit (on/off) setting.
  def auto_commit?
    @connection.auto_commit?
  end

  # Returns the name of the last savepoint in a transactions pool.
  def transactions
    @connection.savepoint
  end

  # Create a new savepoint ID.
  def save
    @connection.transactions.save
  end

  # Release a savepoint ID.
  def release
    @connection.transactions.release
  end

  # Close an active connection.
  def close
    @connection.disconnect
    @connection = nil
  end

private

  # Log message.
  def log(type, msg)
    MonetDB.logger.send type, msg  if MonetDB.logger
  end

end
