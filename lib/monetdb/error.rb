module MonetDB

  class Error < StandardError
  end

  class ConnectionError < Error
  end

  class ProtocolError < Error
  end

  class AuthenticationError < Error
  end

  class CommandError < Error
  end

  class QueryError < Error
  end

end
