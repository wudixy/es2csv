# -*- coding: utf-8 -*-
import operator
import itertools
import os
import csv
import argparse
import time
from calendar import timegm

def str2epoch(datestr, dateformat):
    utc_time = time.strptime(datestr, dateformat)
    epoch_time = timegm(utc_time)
    return str(epoch_time)


def str2unixTime(datestr, dateformat):
    utc_time = time.strptime(datestr, dateformat)
    unixtime = long(time.mktime(utc_time))
    return str(unixtime)


def splitCsv(args):
    f = open(args.file)
    dts = csv.reader(f)
    dts = list(dts)
    if args.headline:
        # 如果包含head字段，剔除
        head = dts.pop(0)
    keyindex = args.splitindex
    if args.dateformat:
        dateformat = args.dateformat
    else:
        dateformat = "%Y-%m-%dT%H:%M:%S.%fZ"
    sa = sorted(dts, key=operator.itemgetter(keyindex))
    for g, v in itertools.groupby(sa, key=operator.itemgetter(keyindex)):
        csvf = open(os.path.join(args.outdir, '%s-%s-%s.csv'%(args.prefix, g, args.suffix)),'w')
        if args.headline and head:
            csvf.write(','.join(head) + '\n')
        for data in list(v):
            if args.dateindex >= 0:
                # data[args.dateindex] = str2epoch(data[args.dateindex], dateformat)
                data[args.dateindex] = str2unixTime(data[args.dateindex], dateformat)
            csvf.write(','.join(data) + '\n')
        csvf.close()
    f.close()


def main():
    # create the top-level parser
    # 公共连接参数，并设置默认值
    parser = argparse.ArgumentParser()
    parser.add_argument('-f','--file',required=True, help='csv File ')
    parser.add_argument('-o','--outdir',default='./', help='output dir,default is ./')
    parser.add_argument('-s','--splitindex',default=0, type=int,  help='split field index,default 0')
    parser.add_argument('-t','--dateindex',type=int, help='change this field to epoch')
    parser.add_argument('-F','--dateformat',default="", help='%Y-%m-%dT%H:%M:%S.%fZ')
    parser.add_argument('-p','--prefix', default="", help='save file name prefix')
    parser.add_argument('-S','--suffix', default="", help='save file name prefix')
    parser.add_argument('-l','--headline', action='store_true', help='output file include head line')
    #parser.set_defaults(func=scsv)
    parser.set_defaults(func=splitCsv)

    args = parser.parse_args()

    args.func(args)

main()
