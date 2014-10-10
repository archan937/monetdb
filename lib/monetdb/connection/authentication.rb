module MonetDB
  class Connection
    module Authentication

      def authenticate
        obtain_server_challenge!

        write authentication_string
        response = read

        case msg_chr(response)
        when MSG_ERROR
          raise MonetDB::AuthenticationError, "Authentication failed: #{response}"
        when MSG_REDIRECT
          authentication_redirect response
        else
          @authentication_redirects = nil
          true
        end
      end

      def obtain_server_challenge!
        config.merge! server_challenge
        assert_supported_protocol!
        select_supported_auth_type!
      end

      def server_challenge
        keys_and_values = [:salt, :server_name, :protocol, :auth_types, :server_endianness, :password_digest_method].zip read.split(":")
        Hash[keys_and_values]
      end

      def assert_supported_protocol!
        unless PROTOCOLS.include?(config[:protocol])
          raise MonetDB::ProtocolError, "Protocol '#{config[:protocol]}' not supported. Only #{PROTOCOLS.collect{|x| "'#{x}'"}.join(", ")}."
        end
      end

      def select_supported_auth_type!
        unless config[:auth_type] = (AUTH_TYPES & (auth_types = config[:auth_types].split(","))).first
          raise MonetDB::AuthenticationError, "Authentication types (#{auth_types.join(", ")}) not supported. Only #{AUTH_TYPES.join(", ")}."
        end
      end

      def authentication_string
        [ENDIANNESS, config[:username], "{#{config[:auth_type]}}#{authentication_hashsum}", LANG, config[:database], ""].join(":")
      end

      def authentication_hashsum
        auth_type, password, password_digest_method = config.values_at(:auth_type, :password, :password_digest_method)

        case auth_type
        when AUTH_MD5, AUTH_SHA512, AUTH_SHA384, AUTH_SHA256, AUTH_SHA1
          password = hexdigest(password_digest_method, password) if config[:protocol] == MAPI_V9
          hexdigest(auth_type, password + config[:salt])
        when AUTH_PLAIN
          config[:password] + config[:salt]
        end
      end

      def hexdigest(method, value)
        Digest.const_get(method).new.hexdigest(value)
      end

      def authentication_redirect(response)
        unless response.split("\n").detect{|x| x.match(/^\^mapi:(.*)/)}
          raise MonetDB::AuthenticationError, "Authentication redirect not supported: #{response}"
        end

        begin
          scheme, userinfo, host, port, registry, database = URI.split(uri = $1)
        rescue URI::InvalidURIError
          raise MonetDB::AuthenticationError, "Invalid authentication redirect URI: #{uri}"
        end

        case scheme
        when "merovingian"
          if (@authentication_redirects ||= 0) < 5
            @authentication_redirects += 1
            authenticate
          else
            raise MonetDB::AuthenticationError, "Merovingian: Too many redirects while proxying"
          end
        when "monetdb"
          config[:host] = host
          config[:port] = port
          connect
        else
          raise MonetDB::AuthenticationError, "Cannot authenticate"
        end
      end

    end
  end
end
