#!/bin/sh
nowdate=$(date +%Y%m%d)
enddate=now

if [ $# -eq 0 ] ; then
    echo 'must set shellscript'
    exit 1
fi
shellscript=$1
if [ $# -eq 1 ] ; then
        begindate=$nowdate
        enddate=$nowdate
fi
if [ $# -eq 2 ] ; then
        begindate=$2
        enddate=$2
fi
if [ $# -eq 3 ] ; then
        begindate=$2
        enddate=$3
fi

echo ${begindate} to ${enddate}


while [ "$begindate" -le "$enddate" ];do
    ml="${shellscript} ${begindate}"
    echo $ml
    $ml
    begindate=`date -d "${begindate} +1 days" +"%Y%m%d"`
    # begindate=$((begindate+86400));
done

