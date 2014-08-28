#
# Handles transactions and savepoints. Can be used to simulate nested transactions.
#

class MonetDB
  class Transaction

    def initialize
      @id = 0
      @savepoint = ""
    end

    def savepoint
      @savepoint = "monetdbsp#{@id}"
    end

    def release
      prev_id
    end

    def save
      next_id
    end

  private

    def next_id
      @id += 1
    end

    def prev_id
      @id -= 1
    end

  end
end
