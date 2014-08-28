#
# Overload the class string to convert monetdb to ruby types.
#

class String

  def getInt
    to_i
  end

  def getFloat
    to_f
  end

  def getString
    gsub(/^"/, "").gsub(/"$/, "")
  end

  # Convert from HEX to the origianl binary data.
  def getBlob
    blob = ""
    scan(/../) { |tuple| blob += tuple.hex.chr }
    blob
  end

  # Ruby currently supports only time formatted timestamps; treat TIME as string.
  def getTime
    gsub(/^"/, "").gsub(/"$/, "")
  end

  # Ruby currently supports only date formatted timestamps; treat DATE as string.
  def getDate
    gsub(/^"/, "").gsub(/"$/, "")
  end

  def getDateTime
    date = split(" ")[0].split("-")
    time = split(" ")[1].split(":")
    Time.gm(date[0], date[1], date[2], time[0], time[1], time[2])
  end

  def getChar
    # Ruby < 1.9 does not have a char datatype
    begin
      ord
    rescue
      self
    end
  end

  def getBool
    if %w(1 y t true).include?(self)
      true
    elsif %w(0 n f false).include?(self)
      false
    end
  end

  def getNull
    if upcase == "NONE"
      nil
    else
      raise "Unknown value"
    end
  end

end
