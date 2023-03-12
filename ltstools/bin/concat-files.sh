#!/bin/bash
#
# concatenate via records for export into primo and library cloud
# 7 march 2023 - cg
#
#
TODAYSDATE=`date -d'today' +"%Y%m%d"`

#LIBRARYCLOUD
find /tmp/JSTORFORUM/transformed -type f |  grep -v 'hollis' | grep -v 'aspace'| xargs cat > /tmp/JSTORFORUM/export/lc/via_export_incr_$TODAYSDATE.tmp
cat /home/jstorforumadm/ltstools/conf/viacollhead.txt /tmp/JSTORFORUM/export/lc/via_export_incr_$TODAYSDATE.tmp /home/jstorforumadm/ltstools/conf/viacolltail.txt > /tmp/JSTORFORUM/export/lc/via_export_incr_$TODAYSDATE.xml
rm -f /tmp/JSTORFORUM/export/lc/via_export_incr_$TODAYSDATE.tmp

#PRIMO
find /tmp/JSTORFORUM/transformed -type f | grep 'hollis' | xargs cat > /tmp/JSTORFORUM/export/primo/via_export_incr_$TODAYSDATE.tmp
cat /home/jstorforumadm/ltstools/conf/taminohead.txt /tmp/JSTORFORUM/export/primo/via_export_incr_$TODAYSDATE.tmp /home/jstorforumadm/ltstools/conf/taminotail.txt > /tmp/JSTORFORUM/export/primo/via_export_incr_$TODAYSDATE.xml
rm -f /tmp/JSTORFORUM/export/primo/via_export_incr_$TODAYSDATE.tmp