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

  def reconnect?
    false
  end

  def check_connectivity!
  end

private

  def socket
    @socket
  end

end
