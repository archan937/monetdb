module MonetDB
  class Connection
    module IO
    private

      def read
        raise ConnectionError, "Not connected to server" unless connected?

        length, last_chunk = read_length
        data, iterations = "", 0

        while (length > 0) && (iterations < 1000) do
          received = socket.recv(length)
          data << received
          length -= received.bytesize
          iterations += 1
        end
        data << read unless last_chunk

        data
      end

      def read_length
        bytes = socket.recv(2).unpack("v")[0]
        [(bytes >> 1), (bytes & 1) == 1]
      end

      def write(message)
        raise ConnectionError, "Not connected to server" unless connected?
        pack(message).each do |chunk|
          socket.write(chunk)
        end
        true
      end

      def pack(message)
        chunks = message.scan(/.{1,#{MAX_MSG_SIZE}}/m)
        chunks.each_with_index.to_a.collect do |chunk, index|
          last_bit = (index == chunks.size - 1) ? 1 : 0
          length = [(chunk.bytes.size << 1) | last_bit].pack("v").force_encoding('utf-8')
          "#{length}#{chunk}"
        end.freeze
      end

    end
  end
end
