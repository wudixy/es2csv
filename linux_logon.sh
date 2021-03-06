#!/bin/sh
if [ $JSONTMP ]; then
    tmpdir=$JSONTMP
else
    tmpdir=./json
fi

if [ $CSVFLODER ]; then
    csvdir=$CSVFLODER
else
    csvdir=./csv
fi

if [ $ESURL ]; then
    url=${ESURL}
else
    url=http://84.239.18.44:9200
fi


spdatadir=${csvdir}/byhost

index=bimap-sa-auditd-*
topic=linux_logon

nowdate=$(date +%Y%m%d)
enddate=now

if [ $# -eq 0 ] ; then
        begindate=$nowdate
        enddate=now
fi
if [ $# -eq 1 ] ; then
        begindate=$1
        enddate=$1
fi
if [ $# -eq 2 ] ; then
        begindate=$1
        enddate=$2
fi


qsl='{
  "_source":{
    "excludes":["messages","message"]
  },
  "size":5000,
  "query": {
    "bool": {
      "must": [
        {"term": {
          "auditd.log.record_type": {
            "value": "USER_LOGIN"
          }
        }},
        {"term": {
          "auditd.log.msg.res": {
            "value": "success"
          }
        }},
        {"range": {
          "logtime": {
            "gte": "#begindate#",
            "lte": "#enddate#",
            "format": "yyyyMMdd"
          }
        }
        }
      ],
      "must_not": [
        {"term": {
          "auditd.log.msg.terminal": {
            "value": "ssh"
          }
        }}
      ]
    }
  }
}'



qsl=${qsl//#begindate#/${begindate}}
qsl=${qsl//#enddate#/${enddate}}
echo ${qsl} > ${tmpdir}/search_${topic}.json

#curl -X GET -u readonly:123456 ${url}/${index}/_search  -H 'Content-Type: application/json' -d @${tmpdir}/search_${topic}.json > ${tmpdir}/${topic}.json
curl -X GET -u ${ESUSER}:${ESPWD} ${url}/${index}/_search  -H 'Content-Type: application/json' -d @${tmpdir}/search_${topic}.json > ${tmpdir}/${topic}.json

python esJson2csv -j ${tmpdir}/${topic}.json -o ${csvdir}/${topic}.csv -l logtime,host,ip,app,ses,addr,auid gethit -f "hit['_source']['logtime'],hit['_source']['bimap']['host'],hit['_source']['bimap']['ip'],hit['_source']['bimap']['app'],hit['_source']['auditd']['log']['ses'],hit['_source']['auditd']['log']['msg']['addr'],hit['_source']['auditd']['log']['auid']"

python splitcsv.py -f ${csvdir}/${topic}.csv -l -s 1 -t 0 -p ${topic} -S $begindate -o ${spdatadir}
