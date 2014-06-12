actions :create, :update

attribute :name,         :kind_of => String , :name_attribute => true
attribute :partitions,   :kind_of => Integer, :default => 1
attribute :replication,  :kind_of => Integer, :default => 1

def initialize(*args)
  super
  @action = :create
end
