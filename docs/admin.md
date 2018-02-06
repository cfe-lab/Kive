---
title: Administration Guide
subtitle: looking after a Kive server
---
# Administration Guide #
This page should help you look after Kive, Slurm, and PostgreSQL. If you don't find your answers here, consider adding
some notes and a link to the documentation you found somewhere else.

## Restarting a compute node ##
Sometimes, we have to restart compute nodes. The most common cause is a memory leak. We can see the memory available
with this command:

    $ bpsh -sap free -h
      0:               total        used        free      shared  buff/cache   available
      0: Mem:            94G         61G         16G        133M         17G         32G
      0: Swap:            0B          0B          0B
      1:               total        used        free      shared  buff/cache   available
      1: Mem:            62G         57G        283M        120M        5.1G        4.7G
      1: Swap:            0B          0B          0B

You can see in the example that node 1 is low on memory, so we're going to drain the Slurm jobs off it and reboot it.
You should replace `n1` with the node name you want to drain.

    sudo `which scontrol` update nodename=n1 state=drain reason="low memory"

Wait until the status changes from draining to drain.

    $ sinfo -N
    NODELIST   NODES     PARTITION STATE 
    n0             1     kive-slow alloc 
    n0             1 kive-clinical alloc 
    n0             1   kive-medium alloc 
    n1             1     kive-slow drain 
    n1             1 kive-clinical drain 
    n1             1   kive-medium drain 

Reboot the compute node, replacing `1` with the node number you just drained.

    sudo `which bpctl` -S 1 --reboot

Watch for it to finish rebooting. Don't panic when it goes into an error state for a few seconds.

    beostatus -c

Put the node back into the Slurm pool.

    sudo `which scontrol` update nodename=n1 state=resume

If there are jobs running in Kive, check that some of them get allocated to the node.

    watch squeue -wn1
