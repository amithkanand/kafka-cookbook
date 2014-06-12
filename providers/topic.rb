class KafkaError < RuntimeError; end

def whyrun_supported?
  true
end

action :create do
  converge_by("Creating topic #{@new_resource.name}") do
    create_topic(@new_resource.name)
    resname = @new_resource.name
    ruby_block "verify_topic" do
      block do
        if !topic_exists?(resname)
          Chef::Log.info "topic was not created #{resname}."        
        else
          Chef::Log.info "topic was created #{resname}"
        end
      end
      action :create
    end
  end  
end

action :update do
  converge_by("Updating configuration for topic #{@new_resource.name}") do
    alter_topic(@new_resource.name)
  end
end

def topic_exists?(topic_name)
  listCmd = "#{@topic_cmd} --list --zookeeper #{@zk_hosts} | grep -i #{@new_resource.name}"
  cmd = Mixlib::ShellOut.new(listCmd)
  cmd.run_command
  begin    
    cmd.error!
    return true
  rescue
    return false
  end
end

def create_topic(topic_name)
  # bin/kafka-topics.sh --create --zookeeper <zookeeper_host:port> --replication-factor 1 --partitions 1 --topic <topic name>
  create_cmd = "#{@topic_cmd} --create --zookeeper #{@zk_hosts} --replication-factor #{@new_resource.replication} --partitions #{@new_resource.partitions} --topic #{@new_resource.name}"
  Chef::Log.info "Creating topic using command :: #{create_cmd}"
  bash "create_kafka_topic"  do
    code create_cmd
    user "root"
    action :run
    not_if { topic_exists?(topic_name) }
  end
end

def alter_topic(topic_name)
  partition_count(topic_name)
  alter_cmd = "#{@topic_cmd} --alter --zookeeper #{@zk_hosts} --topic #{@new_resource.name} --partitions #{@new_resource.partitions}"
  Chef::Log.info "Altering topic using command :: #{alter_cmd}"
  bash "alter_kafka_topic" do
    code alter_cmd
    user "root"
    action :run
    only_if { topic_exists?(topic_name) }
  end
end

def delete_topic(topic_name)

end

def partition_count(topic_name) 
  desc_cmd = `"#{@topic_cmd} --describe --zookeeper #{@zk_hosts} --topic #{@new_resource.name}" | head -1 | awk '{print $2}' | cut -d':' -f2`
  log "Describe command is #{desc_cmd}"
end

def load_current_resource
  @current_resource = Chef::Resource::KafkaTopic.new(@new_resource.name)
  @current_resource.partitions(@new_resource.partitions)
  @current_resource.replication(@new_resource.replication)
  
  # Instance variable declaration
  @zk_hosts = node[:kafka][:zookeeper][:connect].map{|x| x + ":2181"}.join(",")
  if @zk_hosts.empty?
    raise KafkaError.new("Zookeeper hosts not found..")
  end
  @kafka_bin = "#{node[:kafka][:install_dir]}/bin"
  @topic_cmd = "#{@kafka_bin}/kafka-topics.sh"
end

