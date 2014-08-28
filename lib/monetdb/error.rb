class MonetDB

  class Error < StandardError
    def initialize(e)
      $stderr.puts e
    end
  end

  class QueryError < Error
  end

  class DataError < Error
  end

  class CommandError < Error
  end

  class ConnectionError < Error
  end

  class SocketError < Error
  end

  class ProtocolError < Error
  end

end
