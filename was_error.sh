#!/bin/sh
tmpdir=/tmp
csvdir=./csv
spdatadir=./tmp
index=bimap-sa-was-*
url=http://84.239.18.44:9200
topic=was_error

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
  "size": 0,
  "query": {
    "bool": {
      "must": [
        {
          "match_all": {}
        },
        {
          "range": {
          "logtime": {
            "gte": "#begindate#",
            "lte": "#enddate#",
            "format": "yyyyMMdd"
          }
          }
        }
      ],
      "must_not": []
    }
  },
  "_source": {
    "excludes": []
  },
  "aggs": {
    "logtime": {
      "date_histogram": {
        "field": "logtime",
        "interval": "1h",
        "time_zone": "UTC",
        "min_doc_count": 1
      },
      "aggs": {
        "host": {
          "terms": {
            "field": "bimap.host",
            "size": 500,
            "order": {
              "_count": "desc"
            }
          },
          "aggs": {
            "err": {
              "filters": {
                "filters": {
                  "java.lang.OutOfMemoryError": {
                    "query_string": {
                      "query": "java.lang.OutOfMemoryError",
                      "analyze_wildcard": true
                    }
                  },
                  "manyopenfiles": {
                    "query_string": {
                      "query": "\"Too many open files\"",
                      "analyze_wildcard": true
                    }
                  },
                  "J2CA0081E": {
                    "query_string": {
                      "query": "J2CA0081E",
                      "analyze_wildcard": true
                    }
                  },
                  "WSVR0605W": {
                    "query_string": {
                      "query": "WSVR0605W",
                      "analyze_wildcard": true
                    }
                  },
                  "java.lang.StackOverflowError": {
                    "query_string": {
                      "query": "java.lang.StackOverflowError",
                      "analyze_wildcard": true
                    }
                  },
                  "HMGR0152W": {
                    "query_string": {
                      "query": "HMGR0152W",
                      "analyze_wildcard": true
                    }
                  },
                  "J2CA0056I": {
                    "query_string": {
                      "query": "J2CA0056I",
                      "analyze_wildcard": true
                    }
                  },
                  "wfxy": {
                    "query_string": {
                      "query": "\"违反协议\"",
                      "analyze_wildcard": true
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}'


qsl=${qsl//#begindate#/${begindate}}
qsl=${qsl//#enddate#/${enddate}}
echo ${qsl} > ${tmpdir}/search_${topic}.json

curl -X GET -u readonly:123456 ${url}/${index}/_search  -H 'Content-Type: application/json' -d @${tmpdir}/search_${topic}.json > ${tmpdir}/${topic}.json

python esJson2csv -j ${tmpdir}/${topic}.json -o ${csvdir}/${topic}.csv -l "logtime,host,HMGR0152W,J2CA0056I,WSVR0605W,java.lang.OutOfMemoryError,java.lang.StackOverflowError,manyopenfiles,ViolateProtocol" getagg -c -g logtime,host -p err -d HMGR0152W,J2CA0056I,WSVR0605W,java.lang.OutOfMemoryError,java.lang.StackOverflowError,manyopenfiles,wfxy

python splitcsv.py -f ${csvdir}/${topic}.csv -l -s 1 -t 0 -p ${topic} -S $begindate -o ${spdatadir}
