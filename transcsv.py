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
        df_left = df_left.drop_duplicates()
        df_right = df_right.drop_duplicates()
    #dfright = dfright.loc[:,['logtime','ses']]
    #dfright.rename(columns={'logtime':'logouttime'})
    #dfright.columns = ['logouttime', 'ses']
    #print dfright.head()
    f = pd.merge(df_left, df_right, on=keys, how=howjoin)
    return f


"""
csvjoin(r'E:\\ShareDisk\\es2csv.20181210\\es2csv\\csv\\linux_logon.csv',r'E:\\ShareDisk\\es2csv.20181210\\es2csv\\csv\\linux_logout.csv',
['host','ses','ip','app','auid']
)
"""


def output(indp, out, splitindex=None, datefields=None, dateformat='%Y-%m-%dT%H:%M:%S.%fZ', prefix='', suffix='', removedup=True):
    #dfleft = pd.read_csv(r'E:\\ShareDisk\\es2csv.20181210\\es2csv\\csv\\linux_logon.csv', header=0)
    if removedup:
        indp.drop_duplicates()
    if datefields:
        # dateformat="%Y-%m-%dT%H:%M:%S.%fZ"
        def func(x):
            try:
                res = long(time.mktime(time.strptime(x, dateformat)))
            except:
                res = 0
            return res
        datefields = datefields.split(',')
        for d in datefields:
            indp[d] = indp[d].apply(func)
            #indp[d] = indp[d].astype(object)
    if splitindex:
        indp = indp.set_index('host')
        # print indp
        keys = indp.index.unique()
        for k in keys:
            tmp = indp.loc[k]
            #df = df.sort_values(by='logtime')
            tmp.to_csv(os.path.join(out, '%s-%s-%s.csv' %
                                    (prefix, k, suffix)), index=True)
    else:
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
    parser.add_argument('-f', '--csvfile', required=True,
                        help='csv File,first line is head')

    parser.add_argument('-o', '--outputfile', required=True,
                        help='if set splitindex,this option is dir')
    #parser.add_argument('-s','--splitindex',default=0, type=int,  help='split field index,default 0')
    parser.add_argument(
        '-s', '--splitkey', help='if set splitfield, output is mutli csv file by set key')
    parser.add_argument(
        '-d', '--datefields', help='change this field to epoch, mutli filed split by ,')
    parser.add_argument('-F', '--dateformat', default="%Y-%m-%dT%H:%M:%S.%fZ",
                        help='default is %%Y-%%m-%%dT%%H:%%M:%%S.%%fZ')
    parser.add_argument('--prefix', default="",
                        help='save file name prefix, must set splitindex')
    parser.add_argument('--suffix', default="",
                        help='save file name suffix, must set splitindex')
    parser.add_argument('--removedup', action='store_true',
                        help='remove duplicate row')

    parser.add_argument('-j', '--join', action='store_true',
                        help='join 2 csv file by key')
    parser.add_argument('-k', '--keys',  help='keys')
    parser.add_argument('-J', '--joincsvfile', help='join csv file')
    parser.add_argument('-m', '--joinmethod', choices=[
                        'left', 'right', 'outer', 'inner'], default='left', help='join method')

    args = parser.parse_args()
    # args.func(args)
    print args

    infile = pd.read_csv(args.csvfile, header=0)
    if args.join:
        if args.keys and args.joincsvfile:
            rfile = pd.read_csv(args.joincsvfile, header=0)
            dfout = csvjoin(infile, rfile, args.keys.split(
                ','), drop_dupl=True, howjoin=args.joinmethod)

    output(dfout, args.outputfile, splitindex=args.splitkey, datefields=args.datefields,
           dateformat=args.dateformat, prefix=args.prefix, suffix=args.suffix, removedup=args.removedup)


main()

