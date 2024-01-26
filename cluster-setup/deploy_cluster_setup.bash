#!/usr/bin/env bash

# Use rsync to copy the cluster setup code to a target server.

# Command-line parameters:
# prod|test
# e.g.
# ./deploy_cluster_setup.sh prod

# If you need to override the default login/server or upload path, set
# the environment variables CLUSTER_SETUP_LOGIN and/or CLUSTER_SETUP_PATH.
# Check out the version of the code you want before running, as this script
# does not check out a fresh repository; we want this script to transfer over
# config files that would not be in a stock repo.

# Make sure you have (or the account you log into the server with has) appropriate
# permissions on the deployment path.

prod_or_test=$1

echo "Deployed tag/commit/branch $git_tag on $(date)." > deployment_notes.txt
echo 'Output of "git describe":' >> deployment_notes.txt
git describe --tags >> deployment_notes.txt
echo 'Output of "git show --format=oneline --no-patch":' >> deployment_notes.txt
git show --format=oneline --no-patch >> deployment_notes.txt

if [ "$prod_or_test" == "prod" ]; then
  server="kive-int.cfenet.ubc.ca"
else
  server="testkive-int.cfenet.ubc.ca"
fi
server_login=${CLUSTER_SETUP_LOGIN:-"${USER}@${server}"}

deployment_path=${CLUSTER_SETUP_PATH:-"/usr/local/src/cluster-setup"}

rsync -avz --exclude-from deploy_exclude_list.txt -a ./ ${server_login}:${deployment_path}

echo "... done."
