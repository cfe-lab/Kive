#!/bin/sed -f

s/\[YOUR DB NAME HERE\]/kive/
s/\[YOUR DB USER NAME HERE\]/postgres/
s/\[YOUR DB USER PASSWORD HERE\]//
s#MEDIA_ROOT = ''#MEDIA_ROOT = '/tmp'#
s/SLURM_PRIO_KEYWORD = "priority"/SLURM_PRIO_KEYWORD = "prioritytier"/
s/SLURM_PRIO_COLNAME = "PRIORITY"/SLURM_PRIO_COLNAME = "PRIO_TIER"/
s|KIVE_HOME = "/usr/local/share/Kive/kive"|KIVE_HOME = "/home/travis/build/cfe-lab/Kive/kive"|
