require "socket"
require "monetdb/connection/authentication"

module MonetDB
  class Connection
    include Authentication

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

    def setup
      authenticate
      set_timezone_interval
      set_reply_size
    end

    def set_timezone_interval
      return false if @timezone_interval_set

      offset = Time.now.gmt_offset / 3600
      interval = "'+#{offset.to_s.rjust(2, "0")}:00'"

      write "sSET TIME ZONE INTERVAL #{interval} HOUR TO MINUTE;"
      response = read

      raise CommandError, "Unable to set timezone interval: #{response}" if msg?(response, MSG_ERROR)
      @timezone_interval_set = true
    end

    def set_reply_size
      return false if @reply_size_set

      write "Xreply_size #{REPLY_SIZE}\n"
      response = read

      raise CommandError, "Unable to set reply size: #{response}" if msg?(response, MSG_ERROR)
      @reply_size_set = true
    end

    def read
      raise ConnectionError, "Not connected to server" unless connected?

      bytes = socket.recv(2).unpack("v")[0]
      last_chunk = (bytes & 1) == 1
      length = bytes >> 1
      puts "last_chunk: #{last_chunk}, length: #{length}"

      ((length > 0) ? socket.recv(length) : "").tap do |data|
        p data
        data << read unless last_chunk
      end
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

    def msg_chr(string)
      string.empty? ? "" : string[0].chr
    end

    def msg?(string, msg)
      msg_chr(string) == msg
    end

    def log(type, msg)
      MonetDB.logger.send(type, msg) if MonetDB.logger
    end

  end
end
