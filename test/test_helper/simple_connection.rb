class SimpleConnection
  attr_reader :config

  def initialize
    @config = {
      :host => "localhost",
      :port => 50000,
      :username => "monetdb",
      :password => "monetdb"
    }
  end

  def connected?
    !@socket.nil?
  end

private

  def socket
    @socket
  end

end
