This role sets up a Slurm controller node. It builds on the [slurm node]
role.

[slurm node]: ../slurm_node

To set up the slurm controller and database daemons, it will:

- Install and configure a MariaDB server
- Deploy additional configuration files
- Install the Slurm controller components
