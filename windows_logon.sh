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

index=bimap-sa-wineventlog-*
topic=windows_logon

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
  "size":5000,
  "query": {
    "bool": {
      "must": [
        {"term": {
          "event_id": {
            "value": "4624"
          }
        }},
        {"term": {
          "event_data.LogonType": {
            "value": "10"
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
      ]
      
      
    }
  }
}
'


qsl=${qsl//#begindate#/${begindate}}
qsl=${qsl//#enddate#/${enddate}}
echo ${qsl} > ${tmpdir}/search_${topic}.json
curl -X GET -u ${ESUSER}:${ESPWD} ${url}/${index}/_search  -H 'Content-Type: application/json' -d @${tmpdir}/search_${topic}.json > ${tmpdir}/${topic}.json
python esJson2csv -j ${tmpdir}/${topic}.json -o ${csvdir}/${topic}_${begindate}.csv -l "logtime,host,ip,app,TargetLogonId,IpAddress,TargetUsername" gethit -f "hit['_source']['logtime'],hit['_source']['bimap']['host'],hit['_source']['bimap']['ip'],hit['_source']['bimap']['app'],hit['_source']['event_data']['TargetLogonId'],hit['_source']['event_data']['IpAddress'],hit['_source']['event_data']['TargetUserName']"

qsl='{
  "size":5000,
  "query": {
    "bool": {
      "must": [
        {"term": {
          "event_id": {
            "value": "4634"
          }
        }},
        {"term": {
          "event_data.LogonType": {
            "value": "10"
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
      ]
      
      
    }
  }
}
'


topic=windows_logout
qsl=${qsl//#begindate#/${begindate}}
qsl=${qsl//#enddate#/${enddate}}
echo ${qsl} > ${tmpdir}/search_${topic}.json
curl -X GET -u ${ESUSER}:${ESPWD} ${url}/${index}/_search  -H 'Content-Type: application/json' -d @${tmpdir}/search_${topic}.json > ${tmpdir}/${topic}.json
python esJson2csv -j ${tmpdir}/${topic}.json -o ${csvdir}/${topic}_${begindate}.csv -l "logouttime,host,ip,TargetLogonId" gethit -f "hit['_source']['logtime'],hit['_source']['bimap']['host'],hit['_source']['bimap']['ip'],hit['_source']['event_data']['TargetLogonId']"


#python splitcsv.py -f ${csvdir}/${topic}.csv -l -s 1 -t 0 -p ${topic} -S $begindate -o ${spdatadir}
source /root/ipocPython/bin/activate
topic1=windows_logon
python transcsv.py -f ${csvdir}/${topic1}_${begindate}.csv -j -k host,ip,TargetLogonId -J ${csvdir}/${topic}_${begindate}.csv  -o ${csvdir}/windows_login_${begindate}.csv
python transcsv.py -f ${csvdir}/${topic1}_${begindate}.csv -j -k host,ip,TargetLogonId -J ${csvdir}/${topic}_${begindate}.csv --filter "dp['logtime']<=dp['logouttime']" --sortkey host,logtime,TargetLogonId,logouttime --asclist 1,1,1,1 --removeby logtime,host,TargetLogonId -o ${spdatadir} -d logtime,logouttime -s host --prefix windows_login --suffix $begindate 
