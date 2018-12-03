#!/bin/sh
tmpdir=/tmp
csvdir=./csv
spdatadir=./tmp
index=bimap-sa-oracle-*
url=http://84.239.18.44:9200
topic=oracle_logoff

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

qsl=' {
  "aggs": {
    "logtime": {
      "aggs": {
        "host": {
          "aggs": {
            "service_name": {
              "aggs": {
                "userid": {
                  "aggs": {
                    "logoff_dead": {
                      "aggs": {
                        "logoff_lread": {
                          "sum": {
                            "field": "logoff_lread"
                          }
                        },
                        "logoff_lwrite": {
                          "sum": {
                            "field": "logoff_lwrite"
                          }
                        },
                        "logoff_pread": {
                          "sum": {
                            "field": "logoff_pread"
                          }
                        },
                        "sessioncpu": {
                          "sum": {
                            "field": "sessioncpu"
                          }
                        }
                      },
                      "terms": {
                        "field": "logoff_dead",
                        "size": 500
                      }
                    }
                  },
                  "terms": {
                    "field": "userid",
                    "size": 500
                  }
                }
              },
              "terms": {
                "field": "service_name",
                "size": 500
              }
            }
          },
          "terms": {
            "field": "bimap.host",
            "size": 500
          }
        }
      },
      "date_histogram": {
        "field": "logtime",
        "interval": "1h",
        "min_doc_count": 1
      }
    }
  },
  "query": {
    "bool": {
      "must": [
        {
          "match_all": {}
        },
        {
          "match_phrase": {
            "action": {
              "query": "LOGOFF"
            }
          }
        },
        {
          "match_phrase": {
            "type": {
              "query": "aud"
            }
          }
        },
        {
          "range": {
            "logtime": {
              "format": "yyyyMMdd",
              "gte": "#begindate#",
              "lte": "#enddate#"
            }
          }
        }
      ],
      "must_not": []
    }
  },
  "size": 0
}'



qsl=${qsl//#begindate#/${begindate}}
qsl=${qsl//#enddate#/${enddate}}
echo ${qsl} > ${tmpdir}/search_${topic}.json

curl -X GET -u readonly:123456 ${url}/${index}/_search  -H 'Content-Type: application/json' -d @${tmpdir}/search_${topic}.json > ${tmpdir}/${topic}.json

python esJson2csv -j ${tmpdir}/${topic}.json -o ${csvdir}/${topic}.csv -l "logtime,host,service_name,userid,logoff_dead,logoff_lread,logoff_lread,logoff_lwrite,logoff_pread,sessioncpu" getagg -g logtime,host,service_name,userid,logoff_dead -d logoff_lread,logoff_lwrite,logoff_pread,sessioncpu

python splitcsv.py -f ${csvdir}/${topic}.csv -l -s 1 -t 0 -p ${topic} -S $begindate -o ${spdatadir}
