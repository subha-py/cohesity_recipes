# Ransomware simulation script
Simulate.py is a simple ransomware simulation script for simulating ransomware
like behavior on VMware vms to testing and validation.

## Pre-reqs
- A linux or windows vm on vcenter to execute the script in
- At least 5GB of free disk space in that VM.
- IP/Username/password for Cohesity cluster
- Name of the VM as seen in Vcenter
- Python installation
- Connectivity to internet to install library dependencies


The script works as follows,
1. Creates a new Protection Job with inputs specified
2. Triggers first protection run for the job
3. Generates a small set of new files with textual content from included
   dictionary
4. Triggers a new protection run on the same job
5. Repeats steps 3, 4 for 15 times to establish a baseline
6. Encrypts all generated files
7. Triggers a new protection run on the same job

## Inputs
- cluster: ip address of cluster
- user: username for cluster access
- password: password for cluster access
- vm: name of vm as seen in vcenter
- policy: Name of an existing policy on cluster for use
- vcenter: Name of vcenter as registered on the cluster

## Executing the script
1. Untar the downloaded package

   `tar -xvzf ransomware_simulation.tar.gz `
2. `cd ransomware_simulation`
3. `pip install -r requirements.txt`
4. Execute the script

   `nohup python3 simulate.py --cluster <ip> --user <user> --password <password> --vm <vm name> --policy <existing policy name> --vcenter <source name> > output.log 2>&1 &`

From start to finish the script should execute to completion in 60-90 mins
depending on first backup execution time.
