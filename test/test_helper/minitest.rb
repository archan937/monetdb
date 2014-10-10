class MiniTest::Test
  def teardown
    MonetDB.instance_variables.each do |name|
      MonetDB.instance_variable_set name, nil
    end
  end
end
