def self.to_csv(options = {})
  #Assign value to export data from respective column_names 
  CSV.generate(options) do |csv|
    csv << column_names
    all.each do |product|
      csv << product.attributes.values_at(*column_names)
    end
  end
end