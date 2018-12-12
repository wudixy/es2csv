# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import time
from calendar import timegm
import os
import argparse

"""
def str2epoch(datestr, dateformat):
    utc_time = time.strptime(datestr, dateformat)
    epoch_time = timegm(utc_time)
    return str(epoch_time)
"""


def csvjoin(df_left, df_right, keys, drop_dupl=True, howjoin='left'):
    if drop_dupl:
        # 根据所有字段去重
        df_left = df_left.drop_duplicates()
        df_right = df_right.drop_duplicates()
    #dfright = dfright.loc[:,['logtime','ses']]
    #dfright.rename(columns={'logtime':'logouttime'})
    #dfright.columns = ['logouttime', 'ses']
    #print dfright.head()
    f = pd.merge(df_left, df_right, on=keys, how=howjoin)
    return f


#csvjoin(r'E:\\ShareDisk\\es2csv.20181210\\es2csv\\csv\\linux_logon.csv',r'E:\\ShareDisk\\es2csv.20181210\\es2csv\\csv\\linux_logout.csv', ['host','ses','ip','app','auid'])

def filter(dp, expression):
    """根据表达式进行过滤"""
    if expression:
        try:
            outdp = dp[eval(expression)]
        except Exception,e:
            print str(e)
            outdp = dp
        return outdp

def fillnull(dp, filldict):
    try:
        d = eval(filldict)
        outdp = dp.fillna(d)
    except Exception,e:
        print str(e)
        outdp = dp
    return outdp


def output(indp, out, splitindex=None, datefields=None, dateformat='%Y-%m-%dT%H:%M:%S.%fZ', prefix='', suffix='', removedup=True):
    # 有日期类型需要转换
    if datefields:
        # dateformat="%Y-%m-%dT%H:%M:%S.%fZ"
        def func(x):
            try:
                res = long(time.mktime(time.strptime(x, dateformat)))
            except:
                # 对于无法转换的，设置为0,此处需要优化改进
                res = 0
            return res
        datefields = datefields.split(',')
        for d in datefields:
            indp[d] = indp[d].apply(func)
            #indp[d] = indp[d].astype(object)
    # 根据所有字段去重
    if removedup:
        indp = indp.drop_duplicates()
    if splitindex:
        #需要按照指定key拆分文件
        #indp = indp.set_index('host')
        # print indp
        keys = indp[splitindex].unique()
        for k in keys:
            tmp = indp[indp[splitindex]==k]
            #df = df.sort_values(by='logtime')
            tmp.to_csv(os.path.join(out, '%s-%s-%s.csv' %
                                    (prefix, k, suffix)), index=False)
    else:
        #正常输出
        indp.to_csv(out, index=False)


"""
dfleft = pd.read_csv(r'E:\\ShareDisk\\es2csv.20181210\\es2csv\\csv\\tmp.csv', header=0)
output(dfleft,r'E:\\ShareDisk\\es2csv.20181210\\es2csv\\csv',datefields="logtime,abc",splitindex='host')
"""


def main():
    # create the top-level parser
    parser = argparse.ArgumentParser(
        usage='%(prog)s [-h] -f csvfile <-o outputfile [-s splitkey] [-d datefields] [-F dateformat] [--prefix] [--suffix] [--removedup] > [-j join -k joinkey -J joinfile -m joinmethod]'
    )
    parser.add_argument('-f', '--csvfile', required=True, help='csv File,first line is head')

    parser.add_argument('-o', '--outputfile', required=True, help='if set splitindex,this option is dir')
    # 按照指定的key将文件拆分为多个csv文件
    parser.add_argument('-s', '--splitkey', help='if set splitfield, output is mutli csv file by set key')
    # 将date类型的1个或多个字段转换为epoch时间 
    parser.add_argument('-d', '--datefields', help='change this field to epoch, mutli filed split by ,')
    parser.add_argument('-F', '--dateformat', default="%Y-%m-%dT%H:%M:%S.%fZ",
                        help='default is %%Y-%%m-%%dT%%H:%%M:%%S.%%fZ')
    #输出文件的前缀和后缀,仅在拆分文件时使用
    parser.add_argument('--prefix', default="",
                        help='save file name prefix, must set splitindex')
    parser.add_argument('--suffix', default="",
                        help='save file name suffix, must set splitindex')
    # 按照1个或多个key，joincsv文件
    parser.add_argument('-j', '--join', action='store_true',
                        help='join 2 csv file by key')
    parser.add_argument('-k', '--keys',  help='keys')
    parser.add_argument('-J', '--joincsvfile', help='join csv file')
    parser.add_argument('-m', '--joinmethod', choices=[
                        'left', 'right', 'outer', 'inner'], default='left', help='join method')
    # 根据表达式过滤数据
    parser.add_argument('--filter', default=None, help="you can use express like:dp['field_a']>dp['field_b']")
    # 按照sortkey进行排顺序，asclist中0是降序，1是升序
    parser.add_argument('--sortkey', default=None, help="must key split by ,")
    parser.add_argument('--asclist', default=None, help="0:desc;1:asc;must key use , split,like<0,0,1>")
    # 按照指定的1个多多个字段去重
    parser.add_argument('--removeby', default=None, help='remove duplicate row')
    parser.add_argument('--keep', choices=[ 'first', 'last'], default='first', help='keep row')
    # 行完全相同时去重
    parser.add_argument('-r','--removedup', action='store_true', help='remove all field duplicate row')

    # 空值填充
    parser.add_argument('--nulldict', default=None, help="like {'fileda':123,'fieldb':'abc'} fill null value")


    args = parser.parse_args()
    # args.func(args)
    # print args

    infile = pd.read_csv(args.csvfile, header=0)
    if args.join:
        if args.keys and args.joincsvfile:
            rfile = pd.read_csv(args.joincsvfile, header=0)
            dfout = csvjoin(infile, rfile, args.keys.split(
                ','), drop_dupl=True, howjoin=args.joinmethod)

    if args.nulldict:
        dfout = fillnull(dfout,args.nulldict)

    if args.filter:
        dfout = filter(dfout, args.filter)

    if args.sortkey:
        sk = args.sortkey.split(',')
        if args.asclist:
            al = map(int,args.asclist.split(','))
        else:
            # 没有给出，默认全部是按照1升序
            al = [1 for n in range(len(sk))]
        dfout = dfout.sort_values(by=sk,ascending=al)

    if args.removeby:
        dfout = dfout.drop_duplicates(args.removeby.split(','), keep=args.keep)

    output(dfout, args.outputfile, splitindex=args.splitkey, datefields=args.datefields,
           dateformat=args.dateformat, prefix=args.prefix, suffix=args.suffix, removedup=args.removedup)


main()
