module MonetDB
  class Connection
    module Logger
    private

      def log(type, msg)
        MonetDB.logger.send(type, msg) if MonetDB.logger
      end

    end
  end
end
