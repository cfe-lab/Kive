This role sets up the networking infrastructure used by worker nodes, such as:
- the NFS client is installed and NFS volumes are mounted;
- ports are opened for slurmd and ssh;
- the original `/home` directory is moved aside so that `/data/home` on the head node
  will be used as the home directory.
