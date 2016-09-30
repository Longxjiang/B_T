# -*- coding: utf-8 -*-
import pandas as pd
import os
import calendar as cal

# 交易数据存放目录
const_datadir = u'E:\\tick_data\\'

const_resultdir = u'E:\\day_data\\'


# appstarttime = time.clock()
#
# get_all_tick_data()
# appstoptime = time.clock()
#
# print ('Running Time:%s Seconds' %(appstoptime-appstarttime))




def get_monthrange(year):
    """
    输入年份，返回每月月初和月末日期
    :param year:
    :return:
            {
             'month': [1, ....       11,    12]
             'begin': [20160101...20161101, 20161201],
             'end': [20160131...20161130, 20161231],
            }
    """

    FORMAT = "%d%02d%02d"
    dic = {"month": [], "begin": [], "end": []}
    for m in range(1, 13):
        d = cal.monthrange(year, m)
        dic["month"].append(m)
        day = FORMAT % (year, m, 1)
        dic["begin"].append(int(day))
        day = FORMAT % (year, m, d[1])
        dic["end"].append(int(day))
    return dic


#
# dic = get_monthrange(2013)
# print dic['begin'][0]
# print dic['end'][0]
# print dic

# 按月拆分
def split_data(datafile):
    os.chdir(const_datadir)
    month = get_monthrange(2013)
    df = pd.read_csv(datafile)
    df = df.sort_values(['date', 'time'], ascending=[1, 1])
    for i in range(0, 12):
        print i
        month_start = month['begin'][i]
        month_end = month['end'][i]
        # 去一个也的数据
        d = df[(df.date >= month_start) & (df.date <= month_end)]
        resultdir = const_resultdir + datafile + str(month_start) + ".csv"
        print resultdir
        # index=False取出行号
        d.to_csv(resultdir, index=False)


# 按天拆分
# 生成如zn.20131220.csv
def split_data_day(datafile):
    os.chdir(const_datadir)
    month = get_monthrange(2013)
    df = pd.read_csv(datafile)
    df = df.sort_values(['date', 'time'], ascending=[1, 1])
    for i in range(0, 12):
        month_start = month['begin'][i]
        month_end = month['end'][i]
        for j in range(month_start, month_end + 1):
            print j
            d = df[df.date == j]
            datafile = datafile.replace('2013.csv', '')
            datafile = datafile.replace('2014.csv', '')
            resultdir = const_resultdir + datafile.strip() + str(j) + ".csv"
            print resultdir
            if len(d) != 0:
                d.to_csv(resultdir, index=False)
                # index=False取出行号


# 拆分为多天交易记录
def split_all_data(suffix):
    os.chdir(const_datadir)
    file_list = os.listdir(const_datadir)
    datadf = pd.DataFrame()

    for file_name in file_list:
        if file_name.endswith(suffix):
            split_data_day(file_name)


# 生成多天交易记录
# concat_data(20130101.csv):
def concat_data(suffix):
    os.chdir(const_resultdir)
    file_list = os.listdir(const_resultdir)
    datadf = pd.DataFrame()

    print suffix
    for file_name in file_list:
        if file_name.endswith(suffix):
            print file_name
            df = pd.read_csv(file_name)
            datadf = [datadf, df]
            datadf = pd.concat(datadf)
            datadf = datadf.sort_values(['date', 'time'], ascending=[1, 1])
    if len(datadf) != 0:
        datadf.to_csv("all." + suffix, index=False)


def concat_all_data(year):
    month = get_monthrange(year)
    for i in range(0, 12):
        month_start = month['begin'][i]
        month_end = month['end'][i]
        for j in range(month_start, month_end + 1):
            suffix = str(j) + ".csv"
            concat_data(suffix)

# concat_all_data(2013)
