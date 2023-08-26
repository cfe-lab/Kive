# Notes for Other Environments

The roles and playbook in this repository is suitable for setting up a test cluster, but it lacks some features that would make it more ergonomic for a development setup or be outright requirements for managing the production cluster.

This document lists those shortcomings.

## Additional Requirements for a Developent Configuration

- Map kive code instead of or in addition to cloning it? (this saves folks from having to use SSH to access the )

- Add `vagrant` user to the `kive` group
- Add an entry for the `vagrant` user to `pg_hba.conf` so it can access Kive's database
- Add kive virtualenv activation to the Vagrant user's login script
- Install Kive's dev and test requirements
- `sudo -u postgres psql -c 'ALTER USER kive CREATEDB;'  # Only needed to run tests`
- Adapt `setuplib::kive_data` for loading test data from the shared drive


## Additional Requirements for a Production Configuration

- Additional role: `users`
    - create accounts set passwords/shh keys for developers and researchers

- Additional role: `adhoc_node`
    - install ad-hoc analysis tools (blast, etc.)

- Things these playbooks don't cover:
    - setting up networking
    - setting up shared storage drives

- Hostnames should be updated
    - The dev cluster only has one `head` and one `worker` node, and refers to them by hostname several times in playbooks. This should be fixed when managing a multi-worker cluster.

Firewall needs to be opened for httpd (unless this happens when it's installed).

Need to add appropriate users to the Kive group (probably just devs) or just let folks `su kive`.