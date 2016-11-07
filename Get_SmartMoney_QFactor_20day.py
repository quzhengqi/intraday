# -*- coding: utf-8 -*-
"""
Created on Thu Jul 21 08:57:41 2016

@author: zhenkai
"""
import tushare as ts
ts.set_token('b974d4912cd4b2cf9637a940100ae5b872576fcce85abd3db5e4c8173b130c47')
import pandas as pd 
import numpy as np
import os
import glob
from dateutil.parser import parse
'''
计算Q因子
注意用复权的价格和成交量
'''

#读入股票代码:根据现有的高频数据
def getStkcdList(path):
    #读入路径中的文件名称、提取股票代码
    file_path=glob.glob(os.path.join(path,'*.csv'))
    code_list=[]
    for file in file_path:
        #file=file_path[0]
        temp=os.path.basename(file).split('.')
        code=temp[0]+'.'+temp[1]
        code_list.append(code)
    return code_list


def getMonthFirstday(datelist):
    #获取每个月的第一天 
    #datelist [list] 日期序列，str
    firstdays=[]
    for i in range(1,len(datelist)):
        today = datelist[i]
        yesterday = datelist[i-1]
        if parse(today).strftime("%m")!=parse(yesterday).strftime("%m"):
            firstdays.append(yesterday)
    return firstdays 


#计算Q_factor
#window要小于30
def getMktSmartMoneyQFactor(stkcd,window):
    #读入的数据为前复权数据
    bar_data=pd.read_csv(path+'\\'+stkcd+'.csv',encoding='gbk')
    #计算S值，S值越大，代表越聪明
    # 剔除 09：30 的值，股票复权影响
    
    #bar_data = bar_data[bar_data["barTime"]!='09:30']
    
    t_begin = parse(bar_data['dataDate'].drop_duplicates().tolist()[0]).strftime("%Y%m%d")
    t_end = parse(bar_data['dataDate'].drop_duplicates().tolist()[-1]).strftime("%Y%m%d")  
    st = ts.Market()
    df_adjfactor = st.MktEqud(secID=stkcd,beginDate=t_begin,endDate=t_end,field="tradeDate,accumAdjFactor")
    bar_data = pd.merge(bar_data,df_adjfactor,left_on="dataDate",right_on="tradeDate")    
    bar_data["adjclosePrice"] = bar_data["closePrice"]*bar_data["accumAdjFactor"]
    bar_data["adjtotalVolume"] = bar_data["totalVolume"]/bar_data["accumAdjFactor"]
    
    bar_data['return'] = bar_data['adjclosePrice'].pct_change(1)*100
    bar_data['S'] = abs(bar_data['return'])/np.sqrt(bar_data['adjtotalVolume'])
    
    #计算Q值的日期从90日之后开始,剔除上市3个月以内的数据
    #data 中index日期是对应着截止到当日计算的Q值
    q_dates = getMonthFirstday(bar_data['dataDate'].drop_duplicates().tolist()[90:])
    #q_dates = bar_data['dataDate'].drop_duplicates().tolist()[90:]
    all_dates = bar_data['dataDate'].drop_duplicates().tolist()[60:]
        
    data=pd.DataFrame(index=q_dates,columns=['Q'])
    for i in range(len(q_dates)):
        #i=1
        end_date = q_dates[i]
        begin_date = all_dates[all_dates.index(end_date)-window+1]
        tmp_data = bar_data[(bar_data['dataDate']<=end_date)&(bar_data['dataDate']>=begin_date)]
        #排除停牌影响，四分之一以上时间停牌，不计算Q因子
        if len(tmp_data)==0 or len(tmp_data[tmp_data['adjtotalVolume']==0])>window*60:
            continue 
        try:
            #计算window期间的平均成本
            vwap_all=tmp_data['totalValue'].sum()/tmp_data["adjtotalVolume"].sum()
        except:
            continue
        #剔除没有成交量的，或者Q=0的，，why?
        #在有信息含量的成交量里面，选取前20%,统计聪明的钱成本
        tmp_data = tmp_data[(tmp_data['S']>0)&(tmp_data['S']<np.inf)]
        if len(tmp_data)==0:
            continue 
        tmp_data.sort_values(by='S',inplace=True,ascending=False)
        tmp_data['accumVol']=tmp_data['adjtotalVolume'].cumsum()
        smart_vol=tmp_data['accumVol'].values[-1]*0.2
        tmp_data = tmp_data[tmp_data['accumVol']<=smart_vol]
        try:
            vwap_smart = tmp_data['totalValue'].sum()/tmp_data['adjtotalVolume'].sum()   
        except:
            continue
        data['Q'][end_date]=vwap_smart/vwap_all
    return data 


def getDailyQFactorAll(stock_list,file_name,window):
    #获取到所有股票的Q因子值
    q_factor = pd.DataFrame()
    #q_factor.to_csv(file_name)
    for stkcd in stock_list:
        tmp_q = getMktSmartMoneyQFactor(stkcd,window).reset_index()
        tmp_q.columns=['tradeDate',stkcd]
        
        q_factor=pd.read_csv(file_name)
        if q_factor.empty:
            q_factor=tmp_q
        else:
            #读入csv数据时候，会多出来列
            q_factor = q_factor[q_factor.columns[1:]]
            q_factor = q_factor.merge(tmp_q, on='tradeDate',how='outer')
        q_factor.sort_values(by='tradeDate',inplace=True,ascending=True)
        q_factor.to_csv(file_name)
        print(stkcd)
    return q_factor
    
file_name = r'E:\我的策略\Qfactor\QfactorData\Qfactor.csv' 
path='E:\我的策略\基础数据\分钟数据\data'
stock_list=getStkcdList(path)
a=getDailyQFactorAll(stock_list,file_name,window=10)
stkcd = stock_list[0]





