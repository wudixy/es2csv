# -*- coding: utf-8 -*-
import json
import argparse


flist = "hit['_source']['bimap']['host'],\
hit['_source']['bimap']['ip'],\
hit['_source']['bimap']['app'],\
hit['_source']['auditd']['log']['ses'],\
hit['_source']['auditd']['log']['msg']['addr'],\
hit['_source']['auditd']['log']['auid']"

grouplist = ['byhost','bysname','byuid']
valuelist = ['lread','lwrite']

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
    for hit in hits:
        sfmt = ""
        values = []
        for fl in fieldlist.split(','):
            sfmt = sfmt + "%s,"
            values.append(eval(fl))
        print sfmt[:-1] % tuple(values)
    f.close()


def getAgg(jsobj,glist,vlist,data):
    # check key in group by list
    flag = set(jsobj.keys()) & set(glist)
    # if key in group by list,get key value and getAgg
    if flag:
        flag = flag.pop()
        #print flag
        tmpjs = jsobj[flag]['buckets']
        for b in tmpjs:
            data[flag]= b['key']
            getAgg(b,glist,vlist,data)
            #print data
    else:
        # if key not in group by list,get value
        formatstr= ''
        for v in vlist:
            data[v] = jsobj[v]['value']
        values = []
        for g in glist:
            formatstr = formatstr + '%s,'
            values.append(data[g])
        for v in vlist:
            formatstr = formatstr + '%s,'
            values.append(data[v])
        print formatstr[:-1] % tuple(values)

def hit2csv(args):
    jsfile = args.jsonfile
    if args.fields:
        fieldlist = args.fields
    elif args.ffile:
        fieldlist = readFlist(args.ffile)
    #print fieldlist
    readESJson(jsfile, fieldlist)

def aggJson2csv(args):
    jsfile = args.jsonfile
    glist = args.glist.split(',')
    vlist = args.vlist.split(',')

    f = open(jsfile)
    try:
        js = json.load(f)
    except Exception,e:
        print str(e)
        exit(1)
    js = js['aggregations']
    data={}
    getAgg(js,['byhost','bysname','byuid'],['lread','lwrite'],data)


def main():
    # create the top-level parser
    # 公共连接参数，并设置默认值
    parser = argparse.ArgumentParser()
    parser.add_argument('-jsonfile', default="es.json", help='json File ')

    #添加子命令
    subparsers = parser.add_subparsers(title="get hits",description="valid subcommands",help='sub-command help')
    # create the parser for the "action" command
    # get hits
    parser_a = subparsers.add_parser('gethit', help='get hit detail')
    parser_a.add_argument('-fields', default=None, help='field list string')
    parser_a.add_argument('-ffile', default=None,help='field list file')
    parser_a.set_defaults(func=hit2csv)

    #get agg 设置
    parser_b = subparsers.add_parser('getagg', help='export help')
    parser_b.add_argument('-glist',required=True, default=None, help='group by field list;split by ,')
    parser_b.add_argument('-vlist',required=True, default=None, help='value by field list;split by ,')
    parser_b.set_defaults(func=aggJson2csv)


    args = parser.parse_args()

    args.func(args)

main()
