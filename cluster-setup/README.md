# CfE Cluster Setup

This directory contains code and instructions for setting up a multi-host compute cluster.

## Deployment to Octomore

This procedure, as of December 12, 2023, looks like the following.

### Before you wipe the old machine

If you're planning to restore the data from the old machine after the deployment,
make sure your backups are in order.  System backups are typically kept using `rsnapshot`,
and a backup of the Kive PostgreSQL database is kept using `barman`.  For example,
on our production server, these are kept on a NAS mounted at `/media/dragonite`.

Optionally, if your backups are on a physical drive connected to the machine, to avoid
accidentally damaging or altering the backups, you could physically remove them until the 
setup is complete and you're ready to restore data from them.

There a few files that are worth preserving in particular and having available to you
during the deployment process:

* Preserve copies of your system's `/etc/passwd`, `/etc/group`, and `/etc/shadow`.  This 
  information will be used to populate the new system with the same users and groups
  from the old system.
* Create a dump of the Kive PostgreSQL database using `pg_dumpall`.  As the upgrade may
  involve moving to a newer version of PostgreSQL, we likely can't use the Barman
  backups to migrate from; thus we must do it the "old-fashioned" way.
* Preserve a copy of `/etc/kive/kive_apache.conf` and/or `/etc/kive/kive_purge.conf`.  
  These files contain the database password used by Kive (via `apache2`) to access PostgreSQL.  
  You can also just preserve this password and discard the files, as the files will be 
  recreated by Ansible.
* Preserve a copy of the `barman` user's `.pgpass` file.  This contains the passwords
  used by the `barman` and `streaming_barman` users when connecting to PostgreSQL,
  and keeping these makes it easier to get the database set back up after importing
  the database from the old system.  Likewise here you can also just preserve the passwords
  and discard the file.  (Note that this file will typically *not* be present in the `rsnapshot`
  backups, as the Barman user's home directory is in `/var`, which is not backed up.)

### Install Ubuntu and do basic network setup on the head node

First, manually install Ubuntu Jammy on the head node using an Ubuntu live USB drive.
At most points, follow the defaults.  Some places where you need to fill in some details:

- Create a user with username `ubuntu` when prompted during installation.  This will be
  our "bootstrap" user.
- Choose an appropriate system name for the computer, e.g. "octomore".
- Choose a root drive.  As of the time of writing, there is a 120GB SSD on the system; this
  is an appropriate choice for the root drive.
- Manually set up the LAN-facing interface (probably `eno0`) with IP address 192.168.69.86,
  subnet 192.168.68.0/23, gateway 192.168.68.1, and DHCP server 192.168.168.101.
- Enable SSH when prompted.  You don't need to import any identity at this point.

Note that the completion screen isn't super obvious, so keep an eye for a completion message 
at the top left of the screen at the end of the process.  Once this is done, you can interact 
with the head node via SSH.  

Next, upload the contents of [initialization/head] to the server and run `head_configuration.bash`
using `sudo`.
This sets up the root user's SSH key and `/etc/hosts`, and installs Ansible on the head node.  
Accept the defaults whenever it asks which services should be restarted.
Now that Ansible is available on the root node, most of the rest of the procedure will be done
using Ansible playbooks defined in the [deployment] directory.  Copy the `cluster-setup` directory 
to the head node, e.g. using `rsync -avz`, placing it in a sensible location with the appropriate 
permissions.  If you make changes, you can also use `rsync -avz` to keep them synchronized between
your workstation and the head node.

#### Prepare Ansible configuration

Go to the `deployment/group_vars` directory and create an `all.yaml` file from the
`octomore_template.yaml` file by copying and filling in some details.

For the passwords, you can use a password generator to generate new passwords and secret keys; 
however, it makes sense to use the same PostgreSQL passwords as on the old system.  
These passwords are:

* `kive_db_password`: this is the one preserved from `/etc/kive/kive_apache.conf` 
or `/etc/kive/kive_purge.conf`.
* `barman_password`: this is in the `barman` user's `.pgpass` file.
* `streaming_barman_password`: this is also in the `barman` user's `.pgpass` file.

Some other notable settings that you may need to adjust:

* `kive_allowed_hosts`: this is a JSON-formatted list of IP addresses/URLs that the
web server will respond to requests on.
* `kive_subject_prefix`: this will be prepended to the emails sent by the Kive system.
It's a good idea to include some details on this system, e.g. "Kive server on Octomore",
or "Kive server on developer workstation".
* `kive_purge_start`: sets the threshold for the Kive purge task to perform file cleanup.
* `kive_purge_stop`: sets the stopping threshold for this Kive purge task; that is, a 
purge will stop when the remaining files' total size is under this threshold.
* `kive_log_level`: the logging level, as understood by [Django's logging utilities][DjangoLogging],
used by the purge task.

Then go to `deployment/` and create an `ansible.cfg` from one of the provided templates, 
probably `ansible_octomore.cfg`.  These files will be necessary for Ansible to work.

> Note: all playbooks should be run using `sudo`!

[DjangoLogging]: https://docs.djangoproject.com/en/4.0/topics/logging/

#### General preliminary setup

The first playbook we will run sets up the `/data` partition, so the first thing we do
is find the `/dev/disk/by-id/` entry that corresponds to the drive you want to use as `/data`
and put the *basename* (i.e. the name of the soft link in the directory without the 
`/dev/disk/by-id/` part of the path) into `group_vars/all.yml` as the lone entry in the 
`data_physical_volumes` list.  (Or, if you wish to use several volumes combined into 
one logical volume, put all their names in this list.)  

> If any drives are already recognized by LVM from a previous system, you should 
> delete the logical volumes, volume groups, and physical volumes associated with them.  
> Details of how to do so may be found in [the LVM documentation][UbuntuLVMDocs].
> If there are any [mdadm][https://raid.wiki.kernel.org/index.php/A_guide_to_mdadm]
> RAID arrays on these drives, you may also need to shut those down first using
> `mdadm --stop [array device]`.

[UbuntuLVMDocs]: https://manpages.ubuntu.com/manpages/jammy/en/man8/lvm.8.html

Now we can run the playbook `octomore_preliminary_setup.yaml`.  This sets up the `/data` partition,
prepares some other system stuff on the head node, and configures the internal-facing networking.
With this in place, the playbook should set up an `ext4` volume at `/data` on the drive 
you specified.

#### Set up your backup drive

Next, set up a backup drive for your system.  A sample of how this was done for Octomore
is detailed in `create_backup_filesystem.yaml`.  On another server you might use a 
NAS-based backup solution instead.  The goal in the end is to have a backup drive mounted 
at the path specified in your `group_vars` as `kive_backup_path`; by default this would 
be `/media/backup`.

### Install Ubuntu on the compute nodes

At this point, go back into the server room and install Ubuntu Jammy on the compute nodes.
These machines only have one hard drive, and their ethernet should automatically be set up
by default (the head node provides NAT and DHCP), so this should be a very straightforward
installation.  Again, create a user with username `ubuntu` to be the bootstrap user.

Fetch the SSH public key generated by the root user on the head node during the running of
`head_configuration.bash` and place it in the [initialization/worker] directory on the 
head node as `head_node_root_id_ed25519.pub` (don't commit this file to source control;
it isn't a security risk, but it isn't needed and might cause confusion later).  Make an 
appropriate `/etc/hosts` file for the worker nodes and place it in [initialization/worker] 
as `cluster_hosts`; appropriate templates for both Octomore and Bulbasaur are in that 
directory as `cluster_hosts_octomore` and `cluster_hosts_bulbasaur` respectively, so you can
copy one of those to `cluster_hosts` if you don't need anything customized.

Copy the contents of the [initialization/worker] directory to each compute node, 
including the aforementioned SSH public key.  Then, run `worker_configuration.bash` using 
`sudo`, which will install the necessary packages and set up the necessary SSH access for 
the node to be used with Ansible.

### Annoying detour: reassign the bootstrap user's UID and GID

At this point, your `ubuntu` user on all the machines likely have a UID and GID of 1000.
This may conflict with one of the user accounts that will later be
imported into this machine.  If this is the case, you can run `reassign_bootstrap_user_uid.yaml`.  
You may need to create a *second* bootstrap user to do this, as running the playbook as `ubuntu` 
may fail because the user is currently being used (even if you use `sudo`).  This second bootstrap
user can be removed right after this playbook is done, and you can proceed again as the `ubuntu`
user.

### Import users and groups from the old system

The next playbook to run imports users from the old system.  First, a YAML file must be prepared
using `export_users_and_groups.py` from the old system's `/etc/shadow`, `/etc/passwd`, and 
`/etc/group`.  (A Dockerfile and docker compose file are provided in this directory if you 
need a simple environment with Python 3 to run the script.)  Next, run

    sudo ansible-playbook --extra-vars "@[name of the produced YAML file]" import_users.yaml

This will import user accounts into the head node.  (These will later be synchronized to the
compute node as part of a subsequent playbook.)

From here, you can lock and expire the `ubuntu` user and start using one of the just-imported accounts,
if you have one.  Make sure that your uploaded `cluster-setup` directory is accessible by
the account you're using if you do so.  The `lock_bootstrap_user.yaml` playbook can do this;
modify the `user_name` variable if necessary.

### Get SSL credentials for the webserver

Before you install Kive in the next step, you must get the SSL credentials for the server.
These must be acquired securely from IT or within the software group, and placed into the 
[deployment] directory.  *DO NOT* commit these files to source!

The files needed are:

* `DigiCertCA.crt`: the DigiCert certificate authority (CA) key, which specifies that DigiCert
  issued the key.
* `star_cfe.crt`: the wildcard certificate issued by DigiCert, which certifies that this server
  belongs to the `cfenet.ubc.ca` or `bccfe.ca` domain.
* `star_cfe.key`: our private signing key, used to issue a public key for HTTPS connections.

These will then be used in the next step to configure Apache.

### Set up network drives

Our compute server also requires two network mounts, for `macdatafile` and `RAW_DATA`, in
order for MiCall to run.  The playbook `mount_network_drives.yaml` sets these up; fill
in the required variables in `group_vars/all.yaml`; their names and dummy values are in
`group_vars/octomore_template.yaml`.

### Install Kive

With all of that table-setting in place, the main playbook to run is `kive_setup.yml`.  This is
the "main" playbook, and will take longer to run.

At this point, you should have a fresh, "empty" server, with Kive running.  Several 
`systemd`-based background tasks that perform Kive cleanup and backups should also be 
in place.  If that's your goal, then you can stop here.

### Install FastTree

Our Phylowatch service requires [FastTree] 2.1.9 to be installed on the cluster (at the time
of writing).  This is an older version so the binaries are not directly available on the
FastTree website; rather, we must compile it from [the source code][FastTreeSourceCode].  
At the time of writing, the source code is available on their website, but if this ever
disappears, we maintain a vendored copy on macdatafile in the `Phylowatch` directory
as `FastTree-2.1.9.c`.

[FastTree]: https://microbesonline.org/fasttree/
[FastTreeSourceCode]: https://microbesonline.org/fasttree/FastTree-2.1.9.c

Put this file into the `deployment` directory on the head node, and run the 
`install_fasttree.yaml` playbook to compile and install FastTree.

### Optional (but recommended): install smartmontools

To install the `smartmontools` package, which provides `smartctl`, use the
`install_smartmontools.yaml` playbook (or simply install it using `apt`).

## Restore from an old system

If you are restoring an old system, make the backups available somewhere on
your system; e.g. at `/media/old_data` or a similar mount point.

### Shut down Kive and backup services

First, shut down the Kive purge tasks created in the previous step:

    sudo systemctl stop kive_purge.timer
    sudo systemctl stop kive_purge_synch.timer

Next, shut down the backup tasks that were created in the previous step:

    sudo systemctl stop barman_backup.timer
    sudo systemctl stop rsnapshot_alpha.timer
    sudo systemctl stop rsnapshot_beta.timer
    sudo systemctl stop rsnapshot_gamma.timer

Barman installs a cron job by default at the system level.  For now, disable this
by commenting out the entry in `/etc/cron.d/barman`.

Finally, shut down Kive itself by shutting down the PostgreSQL database and 
webserver:

    sudo systemctl stop apache2
    sudo systemctl stop postgresql@14-main

### Annoying detour 2: set the system locale to "Canada English"

At this point in the Octomore migration, it was discovered that the old database
contents would not properly restore to the new database due to problems with the 
database locale.  The old database had as its locale `en_CA.UTF-8`, which was not
available on the newly-upgraded Octomore.

To this end, the `set_locale_to_canada.yml` playbook was used to enable this 
locale on all nodes, and the database then restored without issue.  If this comes
up again, use this same playbook to correct the issue.

### Restoring the database

Now, restore the Kive data folders from the old backups.  On our prod and dev 
clusters this folder was `/data/kive`; use `rsync -avz` to copy this information 
into place on your new server at wherever you set `kive_media_root` to in your
`group-vars` (by default, `/data/kive/media_root`).  Assuming all has gone correctly 
with importing users and groups, the ownership of the files should be as they were 
on the old system.

Next, move the just-created PostgreSQL "cluster" to a backup location (or simply
delete it if you're very confident).  On a fresh install, the cluster is at
`/var/lib/postgresql/14/main`.  Move this to, for example, `/var/lib/postgresql/14/main_backup`.
Create a fresh empty cluster in the original location using `initdb`:

    sudo -u postgres /usr/lib/postgresql/14/bin/initdb /var/lib/postgresql/14/main

At the same time, we should also move (or delete) the Barman backups created to this point, 
as they are inconsistent with the database that we are about to restore.  Move the Barman 
backup folder to a backup location, and create a fresh backup folder in the same location.  
For example, if the backup folder was at `/media/backup/BarmanDBBackup`:

    sudo mv /media/backup/BarmanDBBackup /media/backup/BarmanDBBackup_original
    sudo mkdir /media/backup/BarmanDBBackup
    sudo chown barman:barman /media/backup/BarmanDBBackup

Next you can restore the database using `psql` as the `postgres` user.  Bring up the database
again (this time with the fresh empty cluster) and use `psql` to load the data:

```
sudo systemctl start postgresql@14-main
sudo -u postgres psql -f [dumped file from the old system] postgres
```

Note that in the `psql` command, we specified the database `postgres`.  This must be 
specified (it's a mandatory parameter to `psql`) but will actually be ignored.

At this point, the database will have been restored to the old settings.  If you didn't
use it before in your Ansible configuration (i.e. in `group_vars/all.yaml`), you should
now either specify the PostgreSQL passwords preserved from the old system in 
`/etc/kive/kive_apache.conf`, `/etc/kive/kive_purge.conf`, and the `barman` user's 
`.pgpass`, or reset the passwords using `psql` as the `postgres` user to the ones you 
used in your Ansible settings.

With the database running and restored, bring Apache back up with `sudo systemctl start apache2`.
If the test Kive website doesn't work, check the PostgreSQL logs for clues, and make sure
that Apache is able to reach the database.  Make sure that the password in `/etc/kive/kive_apache.conf`
and `/etc/kive/kive_purge.conf` is correct and working.

### Restore other old user data

This can be done at the leisure of each user, so long as the old backups are mounted.
Use `rsync -avz` to move whatever user data back into place you like.

### Finish setting up Barman

At this point we can manually verify the last details that Barman needs to 
run correctly.  First, reactivate the Barman cron job by uncommenting
the entry you commented out before in `/etc/cron.d/barman`.  Then check on the 
`barman` configuration by running, as the `barman` user,

    barman check kive

There may be problems with the configuration still.  If so, the Barman log at 
`/var/log/barman/barman.log` and the PostgreSQL logs at `/var/log/
may be helpful in diagnosing the problems.  Some that I experienced
while I was going through the process:

* The `barman` and `streaming_barman` PostgreSQL user passwords may be incorrect,
  resulting in the check showing failures for "PostgreSQL", "pg_basebackup compatible", 
  and "pg_receivexlog compatible".  This happened because I didn't preserve these 
  passwords from before I wiped out the database, so I couldn't use the same passwords 
  for `barman` and `streaming_barman` in my Ansible configuration.
  This can easily be remedied by changing these users' PostgreSQL passwords 
  in `psql` (as the `postgres` system user) with the command `\password [username]`;
  use the passwords in the `barman` system user's `.pgpass` file.
* The "replication slot" entry in the `barman check kive` output may report a failure.
  One possible reason for this is that `barman cron` has not run successfully yet,
  as in the previous steps we had disabled the system-level cron job that runs this
  every minute.  This task is what invokes `barman receive-wal`.  If this appears to
  be the problem, you can manually invoke `barman cron` as the `barman` user.  Or, 
  you can wait one minute for the cron job to run and see if this error clears up.
* The output will also indicate that there are not enough backups in place, which is 
  normal and expected at this point.  These backups will be created by the
  `barman_backup` systemd service.
* The check may still report a failure for "WAL archive".  This is normal, as the WAL
  archiving must be verified for a fresh install, and will be handled below.

Next, verify the WAL archiving.  To do this, as the `barman` user, run

    barman switch-wal --force --archive kive

This may fail at first due to a timeout, but try again if so; it's likely to succeed
eventually if all is configured well.  Check the configuration again to confirm
that things are ready to go.  (Ignore the error caused by there not being enough 
backups in place.)

### Restart Kive and backup services

With everything in place, restart the regularly-scheduled backup `systemd` tasks
and Kive purge tasks using `systemctl start` as the root user:

* `barman_backup`
* `rsnapshot_alpha`
* `rsnapshot_beta`
* `rsnapshot_gamma`
* `kive_purge`
* `kive_purge_synch`

For example, run `sudo systemctl start barman_backup.timer` to start `barman_backup`, and
similarly for the others.

Lastly, bring Kive itself back up by bringing up:

* `postgresql@14-main`
* `apache2`

[initialization/head]: initialization/head
[initialization/worker]: initialization/worker
[initialization]: initialization
[deployment]: ./deployment

## Test Environment

We can use Multipass to bring up a test environment for development purposes, or 
Vagrant.

### Multipass

The [initialization] directory contains templates and scripts for generating cloud-init
files to use when setting up a "head" VM and a "worker" VM.

For the head configuration, you must supply a YAML file containing the names and IPs of 
the compute nodes in the same format as they appear in the Ansible `group_vars`; for example, 
simply copy `deployment/group_vars/default_template.yml` (these values are not hugely useful
for this test deployment anyway).  Specify this as a parameter to the `create_head_user_data.py` 
script and it will generate a `user_data` file suitable for use with Multipass:

    multipass launch --name TestHead --cloud-init [user data file you generated] --mount [path to the cluster-setup directory]:/app

For the worker configuration, you must put the SSH public key generated for the root user
on the "head node" somewhere accessible by whoever you want to run `create_worker_user_data.py`,
and specify it as the parameter.  This creates a `user_data` file suitable for use with 
Multipass: similarly to the above,

    multipass launch --name TestWorker --cloud-init [user data file you generated] --mount [path to the cluster-setup directory]:/app

These commands launch the machines and also mount the `cluster-setup` directory at `/app`
on both nodes.  Now that both machines are online and have IP addresses, you can run
`configure_hosts_file.bash` on the head node to configure its `/etc/hosts` file so that
Ansible will know how to reach the worker node.

### Vagrant

This directory contains a Vagrantfile that describes two VMs (a head node and a
worker node) that can be used to test Ansible playbooks or practice performing
cluster management tasks. Ansible is installed on the `head` node, and this directory
is mounted at `/vagrant`. Playbooks can be edited from the host machine, but should
be run from the `head` node.

You'll need to have [Vagrant] and [VirtualBox] or VMWare installed.
To begin, bring up the Vagrant VMs. This will create two VMs (`head` and
`worker`) and install Ansible on `head`.

    vagrant up

On the head node, run (as root) `setup_ssh_keys.bash` and `setup_ssh_access.bash`; this will
install some dummy keys to enable passwordless SSH from the root user to itself,
which is necessary for Ansible.

On the compute node, run (as root) `setup_ssh_access.bash`, which will allow the head
node's root user to SSH into the compute node without a password.  This is also needed
for Ansible.

With both nodes running, you can use `configure_hosts_file.bash` on the head node,
also as root, to fill in the head node's `/etc/hosts` file so that Ansible will know
how to reach the compute node.

At this point, you can log into the head node and work with the code in this directory
at `/vagrant`.  In particular, the Ansible scripts are located in `/vagrant/deployment`.

To confirm that your Ansible configuration is correct, you can run this command:

    ansible -m ping all

This command runs the Ansible's `ping` module against all hosts, which checks that
they can be accessed.

[Vagrant]: https://www.vagrantup.com/downloads.html
[VirtualBox]: https://www.virtualbox.org/wiki/Downloads

## Using Ansible

`ansible.cfg` contains the configuration for the test environment. Most
importantly, it directs ansible to load its inventory from
`deployment/inventory.ini` instead of from the default location under `/etc`.

From `./deployment`, you can run Ansible commands against the inventoried
hosts (including the head node).

### Architecture (for lack of a better name)

Ansible executes *tasks* against one or more managed machines. Tasks may also
depend on *variables*, *files*, or *templates*. These can also be grouped into *roles*,
which we make use of in this project to help organize our code.

### Running playbooks

Run playbooks using `ansible-playbook`, e.g.

    ansible-playbook kive_setup.yml

For all of our playbooks, you're intended to use `sudo` as well.

#### Debugging a single role

Per [this](https://stackoverflow.com/questions/38350674/ansible-can-i-execute-role-from-command-line)
stack overflow answer, a single role can be run with the following command:

    ansible <hostname> -m include_role -a name=<role name>

This has more verbose output and can be run in isolation, making it suitable
for development and debugging.

### Ansible documentation

#### Essential

- [Concepts](https://docs.ansible.com/ansible/latest/user_guide/basic_concepts.html)
- [Quickstart](https://docs.ansible.com/ansible/latest/user_guide/quickstart.html)

#### Thorough

- [Playbooks](https://docs.ansible.com/ansible/2.3/playbooks.html)
- [How to build your inventory](https://docs.ansible.com/ansible/latest/user_guide/intro_inventory.html#intro-inventory)
- [Creating Reusable Playbooks](https://docs.ansible.com/ansible/latest/user_guide/playbooks_reuse.html)
- [Module Index](https://docs.ansible.com/ansible/latest/modules/list_of_all_modules.html)
- [Best Practices](https://docs.ansible.com/ansible/latest/user_guide/playbooks_best_practices.html#playbooks-best-practices)
- [Interpreter Discovery](https://docs.ansible.com/ansible/latest/reference_appendices/interpreter_discovery.html#interpreter-discovery)

#### Extended

- [Installation](https://docs.ansible.com/ansible/latest/installation_guide/intro_installation.html#installation-guide)
- [Become (privesc)](https://docs.ansible.com/ansible/2.3/become.html)
- ["Dry Run" mode](https://docs.ansible.com/ansible/2.3/playbooks_checkmode.html)
- [Asynchronous Actions and Polling](https://docs.ansible.com/ansible/2.3/playbooks_async.html)
- [Vault](https://docs.ansible.com/ansible/2.3/playbooks_vault.html)

#### Useful modules

- [copy](https://docs.ansible.com/ansible/latest/modules/copy_module.html#copy-module)
- [user](https://docs.ansible.com/ansible/latest/modules/user_module.html#user-module)
- [file](https://docs.ansible.com/ansible/latest/modules/file_module.html#file-module), for creating directories
- [systemd](https://docs.ansible.com/ansible/latest/modules/systemd_module.html#systemd-module)
- [debug](https://docs.ansible.com/ansible/latest/modules/debug_module.html#debug-module)
- [dnf](https://docs.ansible.com/ansible/latest/modules/dnf_module.html#dnf-module) (use instead of `yum`, which is Python2 only)
- [mysql_db](https://docs.ansible.com/ansible/latest/modules/mysql_db_module.html#mysql-db-module)
- [get_url](https://docs.ansible.com/ansible/latest/modules/get_url_module.html)
- [replace](https://docs.ansible.com/ansible/latest/modules/replace_module.html)
- [firewalld](https://docs.ansible.com/ansible/latest/modules/firewalld_module.html)
- [command](https://docs.ansible.com/ansible/latest/modules/command_module.html)
- [Postgresql Modules](https://docs.ansible.com/ansible/latest/modules/list_of_database_modules.html#postgresql)
- [lineinfile](https://docs.ansible.com/ansible/latest/modules/lineinfile_module.html)
- [blockinfile](https://docs.ansible.com/ansible/latest/modules/blockinfile_module.html#blockinfile-module)
- [git](https://docs.ansible.com/ansible/latest/modules/git_module.html#git-module)
- [unarchive](https://docs.ansible.com/ansible/latest/modules/unarchive_module.html)