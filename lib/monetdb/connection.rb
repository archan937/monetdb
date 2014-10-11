require "socket"
require "monetdb/connection/message"
require "monetdb/connection/setup"
require "monetdb/connection/select"

module MonetDB
  class Connection

    include Message
    include Setup
    include Select

    Q_TABLE           = "1" # SELECT operation
    Q_UPDATE          = "2" # INSERT/UPDATE operations
    Q_CREATE          = "3" # CREATE/DROP TABLE operations
    Q_TRANSACTION     = "4" # TRANSACTION
    Q_PREPARE         = "5" # QPREPARE message
    Q_BLOCK           = "6" # QBLOCK message

    MSG_REDIRECT      = "^" # Authentication redirect
    MSG_QUERY         = "&"
    MSG_SCHEMA_HEADER = "%"
    MSG_ERROR         = "!"
    MSG_TUPLE         = "["
    MSG_PROMPT        = ""

    MAX_MSG_SIZE      = 32766
    REPLY_SIZE        = "-1"
    ENDIANNESS        = "BIG"
    LANG              = "sql"

    MAPI_V8           = "8"
    MAPI_V9           = "9"
    PROTOCOLS         = [MAPI_V8, MAPI_V9]

    AUTH_MD5          = "MD5"
    AUTH_SHA512       = "SHA512"
    AUTH_SHA384       = "SHA384"
    AUTH_SHA256       = "SHA256"
    AUTH_SHA1         = "SHA1"
    AUTH_PLAIN        = "PLAIN"
    AUTH_TYPES        = [AUTH_MD5, AUTH_SHA512, AUTH_SHA384, AUTH_SHA256, AUTH_SHA1, AUTH_PLAIN]

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
      setup
      true
    end

    def connected?
      !socket.nil?
    end

    def disconnect
      socket.disconnect if connected?
      @socket = nil
    end

  private

    def config
      @config
    end

    def socket
      @socket
    end

    def log(type, msg)
      MonetDB.logger.send(type, msg) if MonetDB.logger
    end

    def read
      raise ConnectionError, "Not connected to server" unless connected?

      length, last_chunk = read_length
      puts "last_chunk: #{last_chunk}, length: #{length}"

      data, iterations = "", 0

      while (length > 0) && (iterations < 1000) do
        received = socket.recv(length)
        data << received
        length -= received.bytes.size
        iterations += 1
      end
      p data
      data << read unless last_chunk

      data
    end

    def read_length
      bytes = socket.recv(2).unpack("v")[0]
      [(bytes >> 1), (bytes & 1) == 1]
    end

    def write(message)
      raise ConnectionError, "Not connected to server" unless connected?
      p message
      pack(message).each do |chunk|
        p chunk
        socket.write(chunk)
      end
      true
    end

    def pack(message)
      chunks = message.scan(/.{1,#{MAX_MSG_SIZE}}/m)
      chunks.each_with_index.to_a.collect do |chunk, index|
        last_bit = (index == chunks.size - 1) ? 1 : 0
        length = [(chunk.size << 1) | last_bit].pack("v")
        "#{length}#{chunk}"
      end.freeze
    end

  end
end
