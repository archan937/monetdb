require "digest/md5"
require "digest/sha1"
require "digest/sha2"

class MonetDB
  class Hasher

    def initialize(method, pwd)
      case method.upcase
      when "SHA1"
        @hashfunc = Digest::SHA1.new
        @hashname = method.upcase
      when "SHA256"
        @hashfunc = Digest::SHA256.new
        @hashname = method.upcase
      when "SHA384"
        @hashfunc = Digest::SHA384.new
        @hashname = method.upcase
      when "SHA512"
        @hashfunc = Digest::SHA512.new
        @hashname = method.upcase
      else
        @hashfunc = Digest::MD5.new
        @hashname = "MD5"
      end
      @pwd = pwd
    end

    # Returns the hash method
    def hashname
      @hashname
    end

    # Compute hash code
    def hashsum
      @hashfunc.hexdigest(@pwd)
    end

  end
end
