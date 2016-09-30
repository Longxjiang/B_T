# -*- coding: utf-8 -*-
import pandas as pd
import matplotlib.pyplot as plt
import os


def addpriceppt(df):
    # 当前累计成交额
    accdf = df[['date', 'time', 'accturnover', 'accvolume', 'ask1', 'bid1']].copy()
    # 上一tick累计成交额
    preaccdf = pd.DataFrame({'date': 0, 'time': 0, 'accturnover': 0, 'accvolume': 0, 'ask1': 0, 'bid1': 0}, index=[0])
    preaccdf = preaccdf.append(accdf.iloc[:-1], ignore_index=True)

    # 当前成交额 = 当前累计成交额 - 上一tick累计成交额
    nowdf = accdf.assign(curturnover=(accdf - preaccdf).copy().accturnover)
    nowdf = nowdf.assign(curvolume=(accdf - preaccdf).copy().accvolume)

    # 当前成交均价=当前成交额/当前成交量
    pricedf = nowdf.assign(price=(nowdf['curturnover'] / nowdf['curvolume']))
    baseprice = pricedf.get_value(0, 'price')
    pptdf = pricedf.assign(priceppt=(pricedf['price'] - baseprice) * 100 / baseprice)
    return pptdf

    # 增加均值列


def addmean(df, colname, window=10):
    newcol = df[colname].rolling(window=window).mean()
    colname = colname + str(window) + 'mean'
    df[colname] = newcol
    return df


def addsignal(df, colname):
    prevalue = df.iloc[0][colname]
    presign = abs(prevalue) / prevalue
    signals = [0]

    for i in range(1, len(df)):

        if df.iloc[i][colname] == 0:
            signals.append(0)
            continue

        nowsign = abs(df.iloc[i][colname]) / df.iloc[i][colname]
        # 负负得正，正正得正，同号相乘得正,意味着符号未发生变化,signal = 0
        if nowsign * presign > 0:
            signals.append(0)
        # 符号可能由正到负
        # 也有可能由负到正
        elif nowsign * presign < 0:
            if presign > 0:
                # 由正到负，卖出信号
                signals.append(-1)
            else:
                # 由负到正，买入信号
                signals.append(1)
        presign = nowsign

    df['signal'] = signals

    return df


def backtest(filename, window=500):
    rudf = pd.read_csv(filename)
    rupptdf = addpriceppt(rudf)
    rumeandf = addmean(rupptdf, 'priceppt', window)
    rumeandf = rumeandf.dropna()
    colnamemean = 'priceppt' + str(window) + 'mean'
    diffdf = rumeandf.assign(diff=rumeandf['priceppt'] - rumeandf[colnamemean])
    signaldf = addsignal(diffdf, 'diff')

    # signaldf[['priceppt', colnamemean,'signal']].plot()
    buydf = signaldf[signaldf['signal'] > 0]
    selldf = signaldf[signaldf['signal'] < 0]

    if len(buydf) > len(selldf):
        buydf = buydf[:-1]
    elif len(buydf) < len(selldf):
        selldf = selldf[:-1]
    else:
        pass

    # print len(selldf)
    # print len(buydf)

    # 计算盈亏
    buydf = buydf.assign(buyfee=buydf['ask1'])
    selldf = selldf.assign(sellfee=selldf['bid1'])

    buytotalfee = buydf['buyfee'].sum()
    selltotalfee = selldf['sellfee'].sum()

    status = ''
    if selltotalfee > buytotalfee:
        status = 'good'
    else:
        status = 'bad'

    # print 'buy: ' + str(buytotalfee)
    # print 'sell: ' + str(selltotalfee)
    #
    # print 'net: ' + str(selltotalfee - buytotalfee)
    # plt.show()

    resultdic = {}
    resultdic['filename'] = filename
    resultdic['buyfee'] = -1 * buytotalfee
    resultdic['sellfee'] = selltotalfee
    resultdic['profit'] = abs(selltotalfee) - abs(buytotalfee)
    resultdic['result'] = status
    resultdic['brokerage'] = (abs(selltotalfee) + abs(buytotalfee)) * 0.45 / 10000
    resultdic['net_profit'] = resultdic['profit'] - resultdic['brokerage']
    resultdic['mean'] = window
    resultdic['volamount-oneside'] = len(selldf)

    resultdf = pd.DataFrame(resultdic, index=[0])
    return resultdf


const_datadir = "E:\\day_data\\"
df = pd.DataFrame()
os.chdir(const_datadir)
file_list = os.listdir(const_datadir)
for file in file_list:
    if file.startswith('ru.201301'):
        for i in range(1, 15):
            print file
            print i
            intervals = i * 120
            p = backtest(file, intervals)
            df = df.append(p, ignore_index=True)
            print df.head(10)
            print len(df)
df = df[['filename', 'mean','buyfee','sellfee','profit','volamount-oneside','brokerage','net_profit','result' ]]
df.to_excel("E:\\backtestdir\\total.xls")
print 'ok'