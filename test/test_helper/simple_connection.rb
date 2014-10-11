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

end
