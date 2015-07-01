require "socket"
require "monetdb/connection/io"
require "monetdb/connection/messages"
require "monetdb/connection/setup"
require "monetdb/connection/query"
require "monetdb/connection/logger"

module MonetDB
  class Connection

    include IO
    include Messages
    include Setup
    include Query
    include Logger

    Q_TABLE        = "1" # SELECT statement
    Q_UPDATE       = "2" # INSERT/UPDATE statement
    Q_CREATE       = "3" # CREATE/DROP TABLE statement
    Q_TRANSACTION  = "4" # TRANSACTION
    Q_PREPARE      = "5" # QPREPARE message
    Q_BLOCK        = "6" # QBLOCK message

    MSG_PROMPT     = ""
    MSG_ERROR      = "!"
    MSG_REDIRECT   = "^"
    MSG_QUERY      = "&"
    MSG_SCHEME     = "%"
    MSG_TUPLE      = "["

    ENDIANNESS     = "BIG"
    LANG           = "sql"
    REPLY_SIZE     = "-1"
    MAX_MSG_SIZE   = 32766

    MAPI_V8        = "8"
    MAPI_V9        = "9"
    PROTOCOLS      = [MAPI_V8, MAPI_V9]

    AUTH_MD5       = "MD5"
    AUTH_SHA512    = "SHA512"
    AUTH_SHA384    = "SHA384"
    AUTH_SHA256    = "SHA256"
    AUTH_SHA1      = "SHA1"
    AUTH_PLAIN     = "PLAIN"
    AUTH_TYPES     = [AUTH_MD5, AUTH_SHA512, AUTH_SHA384, AUTH_SHA256, AUTH_SHA1, AUTH_PLAIN]

    def initialize(config = {})
      @config = {
        :host => "localhost",
        :port => 50000,
        :username => "monetdb",
        :password => "monetdb"
      }.merge(
        config.inject({}){|h, (k, v)| h[k.to_sym] = v; h}
      )
    end

    def connect
      disconnect if connected?
      @socket = TCPSocket.new config[:host], config[:port].to_i
      socket.setsockopt Socket::SOL_SOCKET, Socket::SO_KEEPALIVE, true
      socket.set_encoding 'utf-8'
      setup
      true
    end

    def disconnect
      socket.close if connected?
      @socket = nil
    end

    def connected?
      !socket.nil?
    end

    def reconnect?
      !!config[:reconnect]
    end

    def check_connectivity!
      connect if reconnect? && !connected?
    end

  private

    def config
      @config
    end

    def socket
      @socket
    end

  end
end
