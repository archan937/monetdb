module MonetDB
  class Connection
    module Messages

      def msg_chr(string)
        string.empty? ? "" : string[0].chr
      end

      def msg?(string, msg)
        msg_chr(string) == msg
      end

    end
  end
end
