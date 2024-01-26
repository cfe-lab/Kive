This role performs the tasks that are shared by the Kive [server node] and
[worker nodes]. The roles for those kinds of nodes both depend on this role.

[server node]: ../kive_server
[worker nodes]: ../kive_worker

It uses the [singularity node](../singularity_node) role to install Singularity.

These include:

- Creating a `kive` system user
- Creating the directories that Kive requires
- Fetching the Kive source code
- Installing dependencies (both OS and PyPI)
