#!/bin/bash
#
# concatenate via records for export into primo and library cloud
# 7 march 2023 - cg
#
#
TODAYSDATE=`date -d'today' +"%Y%m%d"`
TARBALL=viaIncr_$TODAYSDATE.tar.gz

while getopts ":s:d:u:f:" o; do
  case "${o}" in
     s)
       s=${OPTARG}
       ;;
     d)
       d=${OPTARG}
       ;;
     u)
       u=${OPTARG}
       ;;
     f)
       f=${OPTARG}
       ;;
  esac
done
shift $((OPTIND-1))

SETNAME=${s}
HARVESTDATE=${d}
UNTILDATE=${u}
FROMDATE=${f}

if [ -n $HARVESTDATE ] && [ -n $UNTILDATE ]
then
  DATESTAMP=$HARVESTDATE_$UNTILDATE
elif [ -n $HARVESTDATE ] && [ -z $UNTILDATE ]
then
  DATESTAMP=$HARVESTDATE
elif [ -z $HARVESTDATE ] && [ -n $UNTILDATE ]
then
  DATESTAMP=$TODAYSDATE_$UNTILDATE
else
then
  DATESTAMP=$TODAYSDATE
fi


if [-n $SETNAME ]
then
  TARBALL=viaIncr_$SETNAME_$DATESTAMP.tar.gz
  find /tmp/JSTORFORUM/transformed/$SETNAME -type f |  grep -v 'hollis' | grep -v 'aspace'| xargs cat > /tmp/JSTORFORUM/export/lc/via_export_incr_$SETNAME_$DATESTAMP.tmp
  cat /home/jstorforumadm/ltstools/conf/viacollhead.txt /tmp/JSTORFORUM/export/lc/via_export_incr_$SETNAME_$DATESTAMP.tmp /home/jstorforumadm/ltstools/conf/viacolltail.txt > /tmp/JSTORFORUM/export/lc/via_export_incr_$SETNAME_$DATESTAMP.xml
  tar -czvf /tmp/JSTORFORUM/export/lc/$TARBALL /tmp/JSTORFORUM/export/lc/via_export_incr_$SETNAME_$DATESTAMP.xml
  rm -f /tmp/JSTORFORUM/export/lc/via_export_incr_$SETNAME_$DATESTAMP.tmp

  find /tmp/JSTORFORUM/transformed/$SETNAME -type f | grep 'hollis' | xargs cat > /tmp/JSTORFORUM/export/primo/via_export_incr_$SETNAME_$DATESTAMP.tmp
  cat /home/jstorforumadm/ltstools/conf/taminohead.txt /tmp/JSTORFORUM/export/primo/via_export_incr_$SETNAME_$DATESTAMP.tmp /home/jstorforumadm/ltstools/conf/taminotail.txt > /tmp/JSTORFORUM/export/primo/via_export_incr_$SETNAME_$DATESTAMP.xml
  tar -czvf /tmp/JSTORFORUM/export/primo/$TARBALL /tmp/JSTORFORUM/export/primo/via_export_incr_$SETNAME_$DATESTAMP.xml
  rm -f /tmp/JSTORFORUM/export/primo/via_export_incr_$SETNAME_DATESTAMP.tmp
else
  #LIBRARYCLOUD
  find /tmp/JSTORFORUM/transformed -type f |  grep -v 'hollis' | grep -v 'aspace'| xargs cat > /tmp/JSTORFORUM/export/lc/via_export_incr_$TODAYSDATE.tmp
  cat /home/jstorforumadm/ltstools/conf/viacollhead.txt /tmp/JSTORFORUM/export/lc/via_export_incr_$TODAYSDATE.tmp /home/jstorforumadm/ltstools/conf/viacolltail.txt > /tmp/JSTORFORUM/export/lc/via_export_incr_$TODAYSDATE.xml
  tar -czvf /tmp/JSTORFORUM/export/lc/$TARBALL /tmp/JSTORFORUM/export/lc/via_export_incr_$TODAYSDATE.xml
  rm -f /tmp/JSTORFORUM/export/lc/via_export_incr_$TODAYSDATE.tmp

  #PRIMO
  find /tmp/JSTORFORUM/transformed -type f | grep 'hollis' | xargs cat > /tmp/JSTORFORUM/export/primo/via_export_incr_$TODAYSDATE.tmp
  cat /home/jstorforumadm/ltstools/conf/taminohead.txt /tmp/JSTORFORUM/export/primo/via_export_incr_$TODAYSDATE.tmp /home/jstorforumadm/ltstools/conf/taminotail.txt > /tmp/JSTORFORUM/export/primo/via_export_incr_$TODAYSDATE.xml
  tar -czvf /tmp/JSTORFORUM/export/primo/$TARBALL /tmp/JSTORFORUM/export/primo/via_export_incr_$TODAYSDATE.xml
  rm -f /tmp/JSTORFORUM/export/primo/via_export_incr_$TODAYSDATE.tmp
fi