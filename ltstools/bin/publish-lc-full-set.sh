#!/bin/bash
#
# full set publish to library cloud
# 25 april 2023 - cg
#
#
/home/jstorforumadm/ltstools/via/bin/via_export.py full -p lc -f $1 -j $2

if [ $? -eq 0 ]
then
  #success
  exit 0
else
  #failure
  exit -1
fi