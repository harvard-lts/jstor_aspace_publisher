#!/bin/bash
#
# concatenate via records for export into primo and library cloud
# 7 march 2023 - cg
#
#
TODAYSDATE=`date -d'today' +"%Y%m%d"`
TARBALL=viaIncr_$TODAYSDATE.tar.gz

while getopts ":s:d:u:f:l:" o; do
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
     l)
       l=${OPTARG}
       ;;
  esac
done
shift $((OPTIND-1))

SETNAME=${s}
HARVESTDATE=${d}
UNTILDATE=${u}
FROMDATE=${f}
FULLRUN=${l}

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
  DATESTAMP=$TODAYSDATE
fi


if [ -n $SETNAME ]
then
  if [ -n $FULLRUN ]
  then
    #LIBRARYCLOUD
    TARBALL=viafull_$SETNAME_$DATESTAMP.tar.gz
    find /tmp/JSTORFORUM/transformed/$SETNAME -type f |  grep -v 'hollis' | grep -v 'aspace'| xargs cat > /tmp/JSTORFORUM/export/lc/viafull_$SETNAME_$DATESTAMP.tmp
    cat /home/jstorforumadm/ltstools/conf/viacollhead.txt /tmp/JSTORFORUM/export/lc/viafull_$SETNAME_$DATESTAMP.tmp /home/jstorforumadm/ltstools/conf/viacolltail.txt > /tmp/JSTORFORUM/export/lc/viafull_$SETNAME_$DATESTAMP.xml
    tar -czvf /tmp/JSTORFORUM/export/lc/$TARBALL /tmp/JSTORFORUM/export/lc/viafull_$SETNAME_$DATESTAMP.xml
    rm -f /tmp/JSTORFORUM/export/lc/viafull_$SETNAME_$DATESTAMP.tmp

    #PRIMO
    find /tmp/JSTORFORUM/transformed/$SETNAME -type f | grep 'hollis' | xargs cat > /tmp/JSTORFORUM/export/primo/viafull_$SETNAME_$DATESTAMP.tmp
    cat /home/jstorforumadm/ltstools/conf/taminohead.txt /tmp/JSTORFORUM/export/primo/viafull_$SETNAME_$DATESTAMP.tmp /home/jstorforumadm/ltstools/conf/taminotail.txt > /tmp/JSTORFORUM/export/primo/viafull_$SETNAME_$DATESTAMP.xml
    tar -czvf /tmp/JSTORFORUM/export/primo/$TARBALL /tmp/JSTORFORUM/export/primo/viafull_$SETNAME_$DATESTAMP.xml
    rm -f /tmp/JSTORFORUM/export/primo/viafull_$SETNAME_$DATESTAMP.tmp
  else
    #LIBRARYCLOUD
    TARBALL=viaIncr_$SETNAME_$DATESTAMP.tar.gz
    find /tmp/JSTORFORUM/transformed/$SETNAME -type f |  grep -v 'hollis' | grep -v 'aspace'| xargs cat > /tmp/JSTORFORUM/export/lc/via_export_incr_$SETNAME_$DATESTAMP.tmp
    cat /home/jstorforumadm/ltstools/conf/viacollhead.txt /tmp/JSTORFORUM/export/lc/via_export_incr_$SETNAME_$DATESTAMP.tmp /home/jstorforumadm/ltstools/conf/viacolltail.txt > /tmp/JSTORFORUM/export/lc/via_export_incr_$SETNAME_$DATESTAMP.xml
    tar -czvf /tmp/JSTORFORUM/export/lc/$TARBALL /tmp/JSTORFORUM/export/lc/via_export_incr_$SETNAME_$DATESTAMP.xml
    rm -f /tmp/JSTORFORUM/export/lc/via_export_incr_$SETNAME_$DATESTAMP.tmp

    #PRIMO
    find /tmp/JSTORFORUM/transformed/$SETNAME -type f | grep 'hollis' | xargs cat > /tmp/JSTORFORUM/export/primo/via_export_incr_$SETNAME_$DATESTAMP.tmp
    cat /home/jstorforumadm/ltstools/conf/taminohead.txt /tmp/JSTORFORUM/export/primo/via_export_incr_$SETNAME_$DATESTAMP.tmp /home/jstorforumadm/ltstools/conf/taminotail.txt > /tmp/JSTORFORUM/export/primo/via_export_incr_$SETNAME_$DATESTAMP.xml
    tar -czvf /tmp/JSTORFORUM/export/primo/$TARBALL /tmp/JSTORFORUM/export/primo/via_export_incr_$SETNAME_$DATESTAMP.xml
    rm -f /tmp/JSTORFORUM/export/primo/via_export_incr_$SETNAME_$DATESTAMP.tmp
  fi
else
  if [ -n $FULLRUN ]
  then
    TARBALL=viafull_$TODAYSDATE.tar.gz
    #LIBRARYCLOUD
    find /tmp/JSTORFORUM/transformed -type f |  grep -v 'hollis' | grep -v 'aspace'| xargs cat > /tmp/JSTORFORUM/export/lc/viafull_$TODAYSDATE.tmp
    cat /home/jstorforumadm/ltstools/conf/viacollhead.txt /tmp/JSTORFORUM/export/lc/viafull_$TODAYSDATE.tmp /home/jstorforumadm/ltstools/conf/viacolltail.txt > /tmp/JSTORFORUM/export/lc/viafull_$TODAYSDATE.xml
    tar -czvf /tmp/JSTORFORUM/export/lc/$TARBALL /tmp/JSTORFORUM/export/lc/viafull_$TODAYSDATE.xml
    rm -f /tmp/JSTORFORUM/export/lc/viafull_$TODAYSDATE.tmp

    #PRIMO
    find /tmp/JSTORFORUM/transformed -type f | grep 'hollis' | xargs cat > /tmp/JSTORFORUM/export/primo/viafull_$TODAYSDATE.tmp
    cat /home/jstorforumadm/ltstools/conf/taminohead.txt /tmp/JSTORFORUM/export/primo/viafull_$TODAYSDATE.tmp /home/jstorforumadm/ltstools/conf/taminotail.txt > /tmp/JSTORFORUM/export/primo/viafull_$TODAYSDATE.xml
    tar -czvf /tmp/JSTORFORUM/export/primo/$TARBALL /tmp/JSTORFORUM/export/primo/viafull_$TODAYSDATE.xml
    rm -f /tmp/JSTORFORUM/export/primo/viafull_$TODAYSDATE.tmp
  else
    TARBALL=viaIncr_$TODAYSDATE.tar.gz
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
fi