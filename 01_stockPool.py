# -*- coding: utf-8 -*-
"""
Created on Fri Oct 14 10:40:01 2016

@author: Administrator
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
        url = '/api/equity/getSecST.json?field=&secID=&ticker='+ticker+'&beginDate=20020101&endDate=20160831'
        code,result_st = client.getData(url)
        if type(result_st)==dict:
            continue
        if len(temp)==0:
            temp = result_st
        else:
            temp = pd.concat([temp,result_st])
    except:
        wrong_list.append(ticker)
temp.to_csv('ST.csv')
t2=time.time()

print(t2-t1)