# -*- coding: utf-8 -*-
"""
Created on Wed Oct 12 15:08:44 2016

@author: Administrator
"""

"""
日流通市值换手率，
日振幅
"""
import pandas as pd
import os
import glob
import tushare as ts

import time
from WindPy import w
w.start()


year=range(2007,2017)
path="G:\\我的数据\\通联数据\\沪深股票日行情\\Data\\"
startDate='20070101'
endDate='20160930'
tradingDays = pd.Series(w.tdays(startDate, endDate, "").Data[0]).map(lambda x:x.strftime("%Y%m%d")).tolist()
swing,turnover={},{}
for trd in tradingDays:
    print(trd)
    year=trd[:4]
    data = pd.read_csv(path+year+"\\"+trd+'.csv',encoding='gb18030')
    data['swing'] = data['highestPrice']-data['lowestPrice']
    data.index=data['secID']
    swing_temp,turnover_temp={},{}
    swing[trd] = data[['swing','isOpen','secID', 'ticker', 'secShortName', 'exchangeCD']]
    turnover[trd] = data[['turnoverRate','isOpen','secID', 'ticker', 'secShortName', 'exchangeCD']]
swing_wp = pd.Panel(swing)
turnover_wp = pd.Panel(turnover)
swing_wp = swing_wp.transpose(2,0,1)
turnover_wp = turnover_wp.transpose(2,0,1)
pd.to_pickle(swing_wp,"D:\\百度云同步盘\\工作文档\\PYTHON\\intraday\\swing.pkl")
pd.to_pickle(turnover_wp,"D:\\百度云同步盘\\工作文档\\PYTHON\\intraday\\turnover.pkl")
