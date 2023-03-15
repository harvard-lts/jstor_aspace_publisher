#!/bin/bash
#
# full publish to library cloud
# 15 march 2023 - cg
#
#
/home/jstorforumadm/ltstools/via/bin/via_export.py full -p lc

if [ $? -eq 0 ]
then
  #success
  exit 0
else
  #failure
  exit -1
fi