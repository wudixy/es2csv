# -*- coding: utf-8 -*-
import json
import argparse

"""
flist = "hit['_source']['bimap']['host'],\
hit['_source']['bimap']['ip'],\
hit['_source']['bimap']['app'],\
hit['_source']['auditd']['log']['ses'],\
hit['_source']['auditd']['log']['msg']['addr'],\
hit['_source']['auditd']['log']['auid']"

grouplist = ['byhost','bysname','byuid']
valuelist = ['lread','lwrite']
"""

def readFlist(fname):
    """read field list from file"""
    f = open(fname)
    return f.read()


def readESJson(jsfile, fieldlist):
    f = open(jsfile)
    try:
        js = json.load(f)
    except Exception, e:
        print str(e)
        exit
    hits = js['hits']['hits']
    res = []
    for hit in hits:
        # sfmt = ""
        values = []
        for fl in fieldlist.split(','):
            # sfmt = sfmt + "%s,"
            values.append(str(eval(fl)))
        #print sfmt[:-1] % tuple(values)
        res.append(values)
    f.close()
    return res


aggres = []

def getAgg(jsobj,glist,vlist,data,dprefix='',getcount=False):
    # check key in group by list
    flag = set(jsobj.keys()) & set(glist)
    global aggres
    # if key in group by list,get key value and getAgg
    if flag:
        flag = flag.pop()
        #print flag
        tmpjs = jsobj[flag]['buckets']
        for b in tmpjs:
            data[flag]= str(b['key'])
            getAgg(b,glist,vlist,data,dprefix,getcount)
            #print data
    else:
        # if key not in group by list,get value
        formatstr= ''
        if dprefix:
            tmjs = jsobj[dprefix]['buckets']
        else:
            tmjs = jsobj
        for v in vlist:
            if getcount:
                data[v] = str(tmjs[v]['doc_count'])
            else:
                data[v] = str(tmjs[v]['value'])
        values = []
        for g in glist:
            formatstr = formatstr + '%s,'
            values.append(str(data[g]))
        for v in vlist:
            formatstr = formatstr + '%s,'
            values.append(str(data[v]))
        #print formatstr[:-1] % tuple(values)
        aggres.append(values)


def hit2csv(args):
    jsfile = args.jsonfile
    if args.fields:
        fieldlist = args.fields
    #elif args.fieldsFile:
    #    fieldlist = readFlist(args.fieldsFile)
    #print fieldlist
    dts = readESJson(jsfile, fieldlist)
    if args.outputfile:
        f = open(args.outputfile,'w')
        for d in dts:
            f.write(','.join(d)+'\n')
        f.close()

def aggJson2csv(args):
    # print args
    jsfile = args.jsonfile
    glist = args.glist.split(',')
    dlist = args.dlist.split(',')

    f = open(jsfile)
    try:
        js = json.load(f)
    except Exception,e:
        print str(e)
        exit(1)
    js = js['aggregations']
    data={}
    getAgg(js,glist,dlist,data,args.prefix,args.count)
    if args.outputfile:
        f = open(args.outputfile,'w')
        for d in aggres:
            f.write(','.join(d)+'\n')
        f.close()


def main():
    # create the top-level parser
    # 公共连接参数，并设置默认值
    parser = argparse.ArgumentParser()
    parser.add_argument('-j','--jsonfile', default="es.json", help='json File ')
    parser.add_argument('-o','--outputfile', default="search.csv", help='Output File ')

    #添加子命令
    subparsers = parser.add_subparsers(title="get hits",description="valid subcommands",help='sub-command help')
    # create the parser for the "action" command
    # get hits
    parser_a = subparsers.add_parser('gethit', help='get hit detail')
    parser_a.add_argument('-f', '--fields',required=True , help='field list string')
    # parser_a.add_argument('-F', '--fieldsFile', default=None,help='field list file')
    parser_a.set_defaults(func=hit2csv)

    #get agg 设置
    parser_b = subparsers.add_parser('getagg', help='export help')
    parser_b.add_argument('-g', '--glist',required=True, default=None, help='group by field list;split by ,')
    parser_b.add_argument('-d', '--dlist',required=True, default=None, help='value by field list;split by ,')
    parser_b.add_argument('-p', '--prefix', default='', help='data field prefix,default is None space')
    parser_b.add_argument('-c', '--count',action='store_true',  help='get doc_count, not def get value')
    parser_b.set_defaults(func=aggJson2csv)


    args = parser.parse_args()

    args.func(args)

main()
