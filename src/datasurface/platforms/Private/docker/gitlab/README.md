# Gitlab setup

This creates volumes for a gitlib server and a single runner. The volumes points at the local gitlab_vols folder. This makes pulling the default
password and such very easy.

* Make sure to run the ../setup_common.sh first to create common volumes and networks.
* Just use the docker-compose.yml file to start the gitlab server and runner.

## Edit .env file

The main thing is to edit the hostname to match your machines local hostname. Mine is BillyAir.local.

## Root password

You need to change the root password. The initial password is stored in the gitlab_vols/config/initial_root_password file. Copy the 
password in this file and login using your browser with root/PASSWORD. Change the password to something secure.

