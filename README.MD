# README

## esJson2csv.py useage

get ElasticSearch query result(format json) file,change to csv.

参数:
1. -j jsonfile input
2. -o outputfile 
3. -l headline

子命令：

gethit 获取hits数据
```
 python esJson2csv -j abc.json -o abc.csv gethit -f "hit['_source']['logtime'],hit['_source']['bimap']['host'],hit['_source']['bimap']['ip'],               hit['_source']['bimap']['app'],hit['_source']['auditd']['log']['ses'],hit['_source']['auditd']['log']['msg']['addr'],hit['_source']['auditd']['log']['auid']"
 ```

getagg 获取agg的数据
such as:
```
python esJson2csv -j ${tmpdir}/${topic}.json -o ${csvdir}/${topic}.csv getagg -g logtime,host,service_name,userid,logoff_dead -d logoff_lread,logoff_lwrite,logoff_pread,sessioncpu
```


## splitcsv.py useage

splitcsv by field

1. -f inputfile
2. -o outputdir
3. -s splitindex, start from 0
4. -t datetime field index, change string datetime to epoch
5. -F datetime Format,default %Y-%m-%dT%H:%M:%S.%fZ
6. -p outputfile prefix,defautl is null
7. -S outputfile suffix,default is null
8. -l headline flag,if use headline,skip 0 row


## Demo useage

1. source ./esenv.sh
2. ./linux_logon.sh 20181201 to make linux logon info from linuxaudit
3. such as:  oracle_logoff.sh  was_error.sh  windows_logon.sh
4. you can use ./mdays.sh linux_logon.sh 20181201 20181231 to run 201812 datas
