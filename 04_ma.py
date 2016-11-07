# -*- coding: utf-8 -*-
"""
Created on Mon Oct 31 13:00:59 2016

@author: Administrator
"""
"""从通联因子数据(专业版)中提取移动平均线数据。
"""
from WindPy import w
w.start()

tradeDate='20161010'
x = w.wset("sectorconstituent","date="+tradeDate+";sectorid=a001010f00000000")

##判断股票池是否
from dataapi import Client
import pandas as pd
client = Client()
client.init('e97fd48ed3ada633e20848c501fa018db3a52734767bfedc20ce0f1ac3aea723')

url = '/api/equity/getEqu.json?field=&ticker=&secID=&equTypeCD=A&listStatusCD=L'
code, result = client.getData(url)
stocklist = result['ticker'].tolist()
 
import time
t1=time.time()
wrong_list=[]
temp = pd.DataFrame()
for ticker in stocklist:
    print(ticker)
    try:
#        url = '/api/market/getMktStockFactorsDateRangePro.json?field=ticker,tradeDate,pe&secID=&ticker='+ticker+'&beginDate=20020101&endDate=20160831'
#        url = '/api/market/getMktStockFactorsOneDayPro.json?field=ticker,tradeDate&secID=&ticker='+ticker+'&beginDate=20070101&endDate=20160831'
        url = '/api/market/getMktStockFactorsDateRangePro.json?field=ticker,secID,tradeDate,MA5,MA10,MA20&secID=&ticker='+ticker+'&beginDate=20070101&endDate=20161101'
        code,result_st = client.getData(url)
        if type(result_st)==dict:
            continue
        if len(temp)==0:
            temp = result_st
        else:
            temp = pd.concat([temp,result_st])
    except:
        wrong_list.append(ticker)
t2=time.time()

print(t2-t1)

temp.to_pickle("D:\\百度云同步盘\\工作文档\\PYTHON\\intraday\\ma.pkl")
#ma = ma[ma['secID']!='X15030.XSHG']
#ma = pd.read_pickle("D:\\百度云同步盘\\工作文档\\PYTHON\\intraday\\ma.pkl")