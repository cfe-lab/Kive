#!/bin/bash
# Functions for installing components of a Kive system.
set -eu -o pipefail
IFS=$'\t\n'

# Install a PostGreSQL database, configure it for use with
# Kive and Vagrant, and start it.
function setuplib::postgres {
    pushd /root

    echo "========== Installing PostgreSQL =========="
    sudo rpm -Uvh https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
    yum install -q -y postgresql10-server postgresql10-contrib
    /usr/pgsql-10/bin/postgresql-10-setup initdb

    # Order matters for access rules.
    sudo sed -i '0,/^local/s/^local/local all kive      peer map=vagrantkive\n&/' \
        /var/lib/pgsql/10/data/pg_hba.conf

    echo "
    # MAPNAME       SYSTEM-USERNAME         PG-USERNAME
    vagrantkive     vagrant                 kive
    vagrantkive     kive                    kive
    " >> /var/lib/pgsql/10/data/pg_ident.conf
    systemctl enable postgresql-10
    systemctl start postgresql-10

    popd
}

# Install MariaDB (for use with Slurm).
function setuplib::mariadb {
    echo "========== Installing MySQL/MariaDB for Slurm =========="
    yum install -q -y mariadb-server mariadb-devel
    systemctl enable mariadb
    systemctl start mariadb
}

# Install Singularity 2.5.2 from source.
function setuplib::singularity {
    pushd /root

    yum groupinstall -q -y 'Development Tools'
    yum install -q -y libarchive-devel squashfs-tools sqlite-devel

    git clone "https://github.com/singularityware/singularity.git"
    pushd singularity
    git checkout -q tags/2.5.2
    ./autogen.sh
    ./configure
    make dist
    popd

    # Install from rpm instead of directly from make, so it can be uninstalled.
    rpmbuild --quiet -ta singularity/singularity-2.5.2.tar.gz
    yum install -q -y rpmbuild/RPMS/x86_64/singularity-2.5.2-1.el7.centos.x86_64.rpm \
        rpmbuild/RPMS/x86_64/singularity-runtime-2.5.2-1.el7.centos.x86_64.rpm
    rm -rf singularity rpmbuild

    popd
}

# Install Munge and add the shared test key.
function setuplib::munge {
    echo "========== Installing Munge =========="
    yum groupinstall -q -y 'Development Tools'
    yum install -q -y epel-release
    yum install -q -y munge munge-libs munge-devel

    cp /usr/local/share/Kive/vagrant/munge-test.key /etc/munge/munge.key
    chown munge:munge /etc/munge/munge.key
    chmod g-r,o-r /etc/munge/munge.key
    systemctl enable munge
    systemctl start munge
}

# Add Slurm user and directories.
function setuplib::slurm_user {
    echo "========== Adding Slurm user =========="
    useradd --system slurm
    mkdir --parents /var/log/slurm /var/run/slurm /var/lib/slurm /etc/slurm
    chown slurm:slurm /var/log/slurm/ /var/run/slurm/ /var/lib/slurm
    chown -R slurm:slurm /etc/slurm/
}

# Install and configure slurmctld and slurmdbd, the Slurm controler daemon
# and database daemon.
function setuplib::slurm_controller {
    echo "========== Installing Slurm controller and database =========="
    pushd /root

    # Set up config files.
    cp  /usr/local/share/Kive/vagrant/slurm.conf \
        /usr/local/share/Kive/vagrant/cgroup.conf \
        /usr/local/share/Kive/vagrant/slurmdbd.conf \
        /etc/slurm/
    chmod o-r /etc/slurm/slurmdbd.conf

    # Create Slurm database and database user.
    mysql --execute "create user slurm@localhost;"
    mysql --execute "grant all on slurm_acct_db.* TO slurm@localhost;"
    mysql --execute "create database slurm_acct_db;"

    yum groupinstall -q -y 'Development Tools'
    yum install -q -y openssl openssl-devel pam-devel numactl numactl-devel \
        hwloc hwloc-devel lua lua-devel readline-devel rrdtool-devel \
        ncurses-devel man2html libibmad libibumad rpm-build perl-devel

    wget -q https://download.schedmd.com/slurm/slurm-17.11.9-2.tar.bz2
    rpmbuild --quiet -ta slurm-17.11.9-2.tar.bz2
    yum install -q -y \
        rpmbuild/RPMS/x86_64/slurm-17.11.9-2.el7.centos.x86_64.rpm \
        rpmbuild/RPMS/x86_64/slurm-example-configs-17.11.9-2.el7.centos.x86_64.rpm \
        rpmbuild/RPMS/x86_64/slurm-slurmctld-17.11.9-2.el7.centos.x86_64.rpm \
        rpmbuild/RPMS/x86_64/slurm-slurmdbd-17.11.9-2.el7.centos.x86_64.rpm
    rm -rf slurm-17.11.9-2.tar.bz2 rpmbuild

    echo "d /var/run/slurm 0755 slurm slurm" > /usr/lib/tmpfiles.d/slurm.conf
    sed -i -e 's|/var/run/slurmdbd.pid|/var/run/slurm/slurmdbd.pid|' \
        /usr/lib/systemd/system/slurmdbd.service
    sed -i -e 's|/var/run/slurmctld.pid|/var/run/slurm/slurmctld.pid|' \
        /usr/lib/systemd/system/slurmctld.service

    systemctl daemon-reload
    systemctl enable slurmdbd
    systemctl start slurmdbd
    systemctl enable slurmctld
    systemctl start slurmctld

    echo "Adding Slurm cluster"
    sacctmgr -i add cluster localhost
    systemctl restart slurmdbd
    systemctl restart slurmctld

    popd
}

# Install and configure slurmd, the Slurm worker daemon.
function setuplib::slurm_worker {
    echo "========== Installing Slurm worker =========="
    pushd /root

    cp  /usr/local/share/Kive/vagrant/slurm.conf \
        /usr/local/share/Kive/vagrant/cgroup.conf \
        /etc/slurm/

    yum groupinstall -q -y 'Development Tools'
    yum install -q -y openssl openssl-devel pam-devel numactl numactl-devel \
        hwloc hwloc-devel lua lua-devel readline-devel rrdtool-devel \
        ncurses-devel man2html libibmad libibumad rpm-build perl-devel \
        mariadb-devel

    wget -q https://download.schedmd.com/slurm/slurm-17.11.9-2.tar.bz2
    rpmbuild --quiet -ta slurm-17.11.9-2.tar.bz2
    yum install -q -y \
        rpmbuild/RPMS/x86_64/slurm-slurmd-17.11.9-2.el7.centos.x86_64.rpm \
        rpmbuild/RPMS/x86_64/slurm-17.11.9-2.el7.centos.x86_64.rpm
    rm -rf slurm-17.11.9-2.tar.bz2 rpmbuild

    echo "d /var/run/slurm 0755 slurm slurm" > /usr/lib/tmpfiles.d/slurm.conf
    sed -i -e 's|/var/run/slurmd.pid|/var/run/slurm/slurmd.pid|' \
        /usr/lib/systemd/system/slurmd.service

    systemctl daemon-reload
    systemctl enable slurmd
    systemctl start slurmd

    popd
}


function setuplib::apache {
    echo "========== Installing Apache =========="
    yum install -q -y httpd mod_wsgi

    cp /usr/local/share/Kive/vagrant_ubuntu/001-kive.conf /etc/httpd/conf.d/
    sed -e 's/^export //' /usr/local/share/Kive/vagrant_ubuntu/envvars.conf >> /etc/sysconfig/httpd
    # KIVE_SECRET_KEY gets added to /etc/sysconfig/httpd in the Kive section below.

    cp /usr/local/share/Kive/vagrant/purge_apache_logs /usr/sbin
    chmod +x,g-w,o-w /usr/sbin/purge_apache_logs
    chmod g-r,o-r /etc/sysconfig/httpd
    sed -e 's/Listen 80$/Listen 8080/' \
        -e 's/User apache$/User kive/' \
        -e 's/Group apache$/Group kive/' \
        -e 's#ErrorLog "logs/error_log"#ErrorLog "|/usr/sbin/rotatelogs -l -p /usr/sbin/purge_apache_logs /var/log/httpd/error_log.%Y-%m-%d-%H%M%S 15M"#' \
        -e 's#CustomLog "logs/access_log" combined#CustomLog "|/usr/sbin/rotatelogs -l -p /usr/sbin/purge_apache_logs /var/log/httpd/access_log.%Y-%m-%d-%H%M%S 15M" combined#' \
        -i /etc/httpd/conf/httpd.conf
    systemctl enable httpd
    systemctl start httpd
}


function setuplib::kive_user {
    echo "========= Configuring kive user ==========="
    useradd --system --key UMASK=002 kive

    mkdir --parents /home/kive /etc/kive /var/log/kive
    chown kive:kive /home/kive /etc/kive /var/log/kive
    chmod go-rx /home/kive
    chmod 770 /etc/kive /var/log/kive
    chmod g+s /etc/kive /var/log/kive

    # Can't change ownership, but should have lax permissions
    mkdir --parents /data/kive/media_root
}

function setuplib::vagrant_user {
    echo "========= Configuring vagrant user ==========="
    cat /usr/local/share/Kive/vagrant_ubuntu/envvars.conf >> ~vagrant/.bash_profile
    echo ". /opt/venv_kive/bin/activate" >> ~vagrant/.bash_profile
}

# Install Python 3.6
function setuplib::python3 {
    echo "========= Installing Python 3 ==========="
    yum install -q -y centos-release-scl
    yum install -q -y python36 python36-devel
}

function setuplib::mod_wsgi {
    echo "========= Installing Mod WSGI for Python 3 ==========="
    yum install -q -y centos-release-scl
    yum install -q -y rh-python36-mod_wsgi
    cp /opt/rh/httpd24/root/usr/lib64/httpd/modules/mod_rh-python36-wsgi.so /lib64/httpd/modules
    cp /opt/rh/httpd24/root/etc/httpd/conf.modules.d/10-rh-python36-wsgi.conf /etc/httpd/conf.modules.d
    systemctl restart httpd
}

# Prepare a node to be a Kive worker 
function setuplib::kive_worker {
    echo "========= Installing Kive worker components ==========="
    usermod -a -G kive vagrant
    yum groupinstall -q -y 'Development Tools'
    yum install -q -y python-devel libsqlite3x-devel words lsof graphviz graphviz-devel

    python3 -m venv /opt/venv_kive
    . /opt/venv_kive/bin/activate
    python -m pip install --upgrade pip
    pushd /usr/local/share/Kive/
    python -m pip install -r requirements-dev.txt
    popd
}

# Set a node up to host the Kive web application
function setuplib::kive_head {
    echo "========= Installing Kive application ==========="
    python3 -m venv /opt/venv_kive
    cd /usr/local/share/Kive/vagrant
    ./kive_setup.bash requirements-dev.txt
    cd /usr/local/share/Kive/vagrant_ubuntu
    ./dbcreate.sh

    local secretkey
    secretkey="$(python3 -c 'import secrets; print(secrets.token_urlsafe())')"
    echo "
KIVE_SECRET_KEY='$secretkey'
KIVE_MEDIA_ROOT='/data/kive/media_root'
KIVE_ALLOWED_HOSTS='[\"*\"]'
" >> /etc/sysconfig/httpd
}
