# TODO(nknight): what's the NFS situation with our cluster?
# config.vm.synced_folder "../data", "/vagrant_data"

HEAD_IP = "192.168.45.10"
WORKER_IP = "192.168.45.11"

# Copy the test SSH keys into `/home/vagrant/.ssh/`
def add_keys(vm)
  vm.provision "file",
    source: "./setupfiles/vagrant_testkey",
    destination: "/home/vagrant/.ssh/id_ed25519"
  vm.provision "file",
    source: "./setupfiles/vagrant_testkey.pub",
    destination: "/home/vagrant/.ssh/id_ed25519.pub"
  vm.provision "shell", inline: <<-EOS
    chmod 600 /home/vagrant/.ssh/id_ed25519
    chmod 644 /home/vagrant/.ssh/id_ed25519.pub
  EOS
end

# Copy the test SSH public key into `/home/vagrant/.ssh/authorized_keys`
def add_key_access(vm)
  vm.provision "file",
    source: "./setupfiles/vagrant_testkey.pub",
    destination: "/tmp/vagrant_testkey.pub"
  vm.provision "shell", inline: <<-EOS
    cat /tmp/vagrant_testkey.pub >> /home/vagrant/.ssh/authorized_keys
    chmod 600 /home/vagrant/.ssh/authorized_keys
  EOS
end

Vagrant.configure("2") do |config|
  config.vm.box = "centos/8"
  
  config.vm.provider "virtualbox" do |vb|
    vb.gui = false
    vb.memory = "1024"
  end

  config.vm.define :head do |head|
    head.vm.hostname = "head"
    head.vm.network "private_network", ip: HEAD_IP
    add_keys(head.vm)
    add_key_access(head.vm)
    head.vm.provision "shell", path: "./setupfiles/install-ansible.sh"
  end

  config.vm.define :worker do |worker|
    worker.vm.hostname = "worker"
    worker.vm.network "private_network", ip: WORKER_IP
    add_key_access(worker.vm)
  end

  config.vm.provision "shell", inline: <<-EOS
    echo "#{HEAD_IP}\thead\n#{WORKER_IP}\tworker" >> /etc/hosts
  EOS
end
