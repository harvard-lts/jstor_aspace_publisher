#!/bin/bash
#
# full setpublish to primo
# 25 april 2023 - cg
#
#
/home/jstorforumadm/ltstools/via/bin/via_export.py full -p primo -f $1 -j $2

if [ $? -eq 0 ]
then
  #success
  exit 0
else
  #failure
  exit -1
fi