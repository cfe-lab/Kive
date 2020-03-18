#!/usr/bin/env bash

# Exit immediately if any untested command fails.
set -e

cd /root


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




echo ========== Installing Slurm ==========
mkdir --parents /etc/slurm
cp  /usr/local/share/Kive/vagrant/slurm.conf \
    /usr/local/share/Kive/vagrant/cgroup.conf \
    /etc/slurm/

yum install -q -y epel-release

yum groups install -q -y 'Development Tools'

yum install -q -y munge munge-libs munge-devel rng-tools
rngd -r /dev/urandom
create-munge-key -r
systemctl enable munge
systemctl start munge

yum install -q -y openssl openssl-devel pam-devel numactl numactl-devel \
    hwloc hwloc-devel lua lua-devel readline-devel rrdtool-devel \
    ncurses-devel man2html libibmad libibumad rpm-build perl-devel \
    mariadb-devel
wget -q https://download.schedmd.com/slurm/slurm-17.11.9-2.tar.bz2
rpmbuild --quiet -ta slurm-17.11.9-2.tar.bz2
yum install -q -y \
    rpmbuild/RPMS/x86_64/slurm-17.11.9-2.el7.centos.x86_64.rpm \
    rpmbuild/RPMS/x86_64/slurm-example-configs-17.11.9-2.el7.centos.x86_64.rpm \
    rpmbuild/RPMS/x86_64/slurm-slurmd-17.11.9-2.el7.centos.x86_64.rpm
rm -rf slurm-17.11.9-2.tar.bz2 rpmbuild
useradd --system slurm
mkdir /var/log/slurm /var/run/slurm /var/lib/slurm
chown slurm:slurm /var/log/slurm/ /var/run/slurm/ /var/lib/slurm
chown -R slurm:slurm /etc/slurm/
echo "d /var/run/slurm 0755 slurm slurm" > /usr/lib/tmpfiles.d/slurm.conf
sed -i -e 's|/var/run/slurmd.pid|/var/run/slurm/slurmd.pid|' \
    /usr/lib/systemd/system/slurmd.service
systemctl daemon-reload
systemctl enable slurmd
systemctl start slurmd

# sacctmgr -i add cluster localhost
systemctl restart slurmd
