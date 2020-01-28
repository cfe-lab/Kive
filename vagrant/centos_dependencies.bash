#!/usr/bin/env bash

# Exit immediately if any untested command fails.
set -e

cd /root

echo ========== Installing PostgreSQL ==========
sudo rpm -Uvh https://yum.postgresql.org/10/redhat/rhel-7-x86_64/pgdg-centos10-10-2.noarch.rpm
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

echo ========== Installing MySQL/MariaDB for Slurm ==========
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
echo "d /var/run/slurm 0755 slurm slurm" > /usr/lib/tmpfiles.d/slurm.conf
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

echo ========== Installing Apache ==========
# httpd is already installed.
yum install -q -y mod_wsgi

useradd --system --key UMASK=002 kive
mkdir /home/kive /etc/kive /var/log/kive
chown kive:kive /home/kive /etc/kive /var/log/kive
chmod go-rx /home/kive
chmod 770 /etc/kive /var/log/kive
chmod g+s /etc/kive /var/log/kive

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

echo ========= Configuring vagrant user ===========
cat /usr/local/share/Kive/vagrant_ubuntu/envvars.conf >> ~vagrant/.bash_profile
echo ". /opt/venv_kive/bin/activate" >> ~vagrant/.bash_profile
