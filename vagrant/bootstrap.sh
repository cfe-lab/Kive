#!/usr/bin/env bash

# Exit immediately if any untested command fails.
set -e

cd /root

echo ========== Installing PostgreSQL ==========
yum install -q -y postgresql-server postgresql-contrib
postgresql-setup initdb
systemctl enable postgresql
systemctl start postgresql

echo ========== Installing Singularity ==========
yum groupinstall -q -y 'Development Tools'
yum install -q -y libarchive-devel squashfs-tools python-devel sqlite-devel
git clone https://github.com/singularityware/singularity.git
cd singularity
git checkout -q tags/2.5.2
./autogen.sh
./configure
make dist
cd ..

# Install from rpm instead of directly from make, so it can be uninstalled.
rpmbuild --quiet -ta singularity/singularity-2.5.2.tar.gz
yum install -q -y rpmbuild/RPMS/x86_64/singularity-2.5.2-1.el7.centos.x86_64.rpm \
    rpmbuild/RPMS/x86_64/singularity-runtime-2.5.2-1.el7.centos.x86_64.rpm
rm -rf singularity rpmbuild

echo ========== Installing Docker ==========
yum install -q -y yum-utils \
  device-mapper-persistent-data \
  lvm2
yum-config-manager \
    --add-repo \
    https://download.docker.com/linux/centos/docker-ce.repo
yum install -q -y docker-ce
systemctl enable docker
systemctl start docker

echo ========== Installing MySQL/MariaDB ==========
yum install -q -y mariadb-server mariadb-devel
systemctl enable mariadb
systemctl start mariadb

echo ========== Installing Slurm ==========
mkdir --parents /etc/slurm
cp  /usr/local/share/Kive/vagrant/slurm.conf \
    /usr/local/share/Kive/vagrant/cgroup.conf \
    /usr/local/share/Kive/vagrant/slurmdbd.conf \
    /etc/slurm/
mysql --execute "create user slurm@localhost;"
mysql --execute "grant all on slurm_acct_db.* TO slurm@localhost;"
mysql --execute "create database slurm_acct_db;"
yum install -q -y epel-release
yum install -q -y munge munge-libs munge-devel rng-tools
rngd -r /dev/urandom
create-munge-key -r
systemctl enable munge
systemctl start munge
yum install -q -y openssl openssl-devel pam-devel numactl numactl-devel \
    hwloc hwloc-devel lua lua-devel readline-devel rrdtool-devel \
    ncurses-devel man2html libibmad libibumad rpm-build perl-devel
wget -q https://download.schedmd.com/slurm/slurm-17.11.9-2.tar.bz2
rpmbuild --quiet -ta slurm-17.11.9-2.tar.bz2
yum install -q -y \
    rpmbuild/RPMS/x86_64/slurm-17.11.9-2.el7.centos.x86_64.rpm \
    rpmbuild/RPMS/x86_64/slurm-example-configs-17.11.9-2.el7.centos.x86_64.rpm \
    rpmbuild/RPMS/x86_64/slurm-slurmctld-17.11.9-2.el7.centos.x86_64.rpm \
    rpmbuild/RPMS/x86_64/slurm-slurmd-17.11.9-2.el7.centos.x86_64.rpm \
    rpmbuild/RPMS/x86_64/slurm-slurmdbd-17.11.9-2.el7.centos.x86_64.rpm
rm -rf slurm-17.11.9-2.tar.bz2 rpmbuild
useradd --system slurm
mkdir /var/log/slurm /var/run/slurm /var/lib/slurm
chown slurm:slurm /var/log/slurm/ /var/run/slurm/ /var/lib/slurm
chown -R slurm:slurm /etc/slurm/
chmod o-r /etc/slurm/slurmdbd.conf
sed -i -e 's|/var/run/slurmdbd.pid|/var/run/slurm/slurmdbd.pid|' \
    /usr/lib/systemd/system/slurmdbd.service
sed -i -e 's|/var/run/slurmctld.pid|/var/run/slurm/slurmctld.pid|' \
    /usr/lib/systemd/system/slurmctld.service
sed -i -e 's|/var/run/slurmd.pid|/var/run/slurm/slurmd.pid|' \
    /usr/lib/systemd/system/slurmd.service
systemctl daemon-reload
systemctl enable slurmdbd
systemctl start slurmdbd
systemctl enable slurmctld
systemctl start slurmctld
systemctl enable slurmd
systemctl start slurmd

sacctmgr -i add cluster localhost
systemctl restart slurmdbd
systemctl restart slurmctld

echo ========== Installing pip ==========
wget -q https://bootstrap.pypa.io/get-pip.py
python get-pip.py pip==9.0.1
rm get-pip.py

echo ========== Installing Kive ==========
yum install -q -y python-devel libsqlite3x-devel words lsof graphviz graphviz-devel
cd /usr/local/share/Kive/api
python setup.py install
cd ..
mkdir --parents \
    vagrant_ubuntu/media_root_backup/CodeResources \
    vagrant_ubuntu/media_root_backup/Datasets \
    vagrant_ubuntu/media_root_backup/Logs \
    /var/kive/media_root
chown vagrant:vagrant /var/kive/media_root
ln -s /usr/local/share/Kive/vagrant_ubuntu/media_root_backup/* \
    /var/kive/media_root/
ln -s /usr/local/share/Kive/kive/fleet/docker_wrap.py /usr/local/bin/docker_wrap.py
pip install -r requirements-dev.txt
sed -e 's/\[YOUR DB NAME HERE\]/kive/' \
    -e 's/\[YOUR DB USER NAME HERE\]/vagrant/' \
    -e 's/\[YOUR DB USER PASSWORD HERE\]//' \
    -e "s|MEDIA_ROOT = ''|MEDIA_ROOT = '/var/kive/media_root'|" \
    -e 's/SLURM_PRIO_KEYWORD = "priority"/SLURM_PRIO_KEYWORD = "prioritytier"/' \
    -e 's/SLURM_PRIO_COLNAME = "PRIORITY"/SLURM_PRIO_COLNAME = "PRIO_TIER"/' \
    kive/kive/settings_default.py > kive/kive/settings_vagrant.py
if [ ! -f kive/kive/settings.py ]; then
    cp kive/kive/settings_vagrant.py kive/kive/settings.py
fi

cd vagrant_ubuntu
sudo -u vagrant ./dbcreate.sh

# Launch server like this:
# cd /usr/local/share/Kive/kive
# ./manage.py runserver 0.0.0.0:8000
