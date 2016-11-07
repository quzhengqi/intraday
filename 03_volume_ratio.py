# -*- coding: utf-8 -*-
"""
Created on Wed Oct 12 14:01:10 2016

@author: Administrator
"""

"""量比 = 股市开市后平均每分钟的成交量/过去5个交易日平均每分钟成交量之比。
"""
import pandas as pd
import os
import glob
import tushare as ts

import time
from WindPy import w
w.start()


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

def getVolumeRatio(startDate,endDate,path,n=30):
    stocklist = getStkcdList(path)
    tradingDays = pd.Series(w.tdays(startDate, endDate, "").Data[0]).map(lambda x:x.strftime("%Y-%m-%d")).tolist()
    volume_ratio_df =pd.DataFrame(columns=stocklist,index=tradingDays)
    t1=time.time()
    wrong_list=[]
    for stk in stocklist:
        try:
            print(stk)
            data_stk = pd.read_csv(path+stk+".csv",encoding='GB2312')    
            # 取前n分钟数据
            data_stk = data_stk.sort_values(by=['dataDate','barTime'],ascending=[True,True])
            data_stk_n = data_stk.groupby(['dataDate']).head(n)
            data_stk_n_avg = pd.DataFrame(data_stk_n.groupby(['dataDate'])['totalVolume'].mean()) #使用成交量，并考虑复权因子。
            
            #考虑复权因子
            ts.set_token('b974d4912cd4b2cf9637a940100ae5b872576fcce85abd3db5e4c8173b130c47')    
            st = ts.Market()
            df_adjfactor = st.MktEqud(secID=stk,beginDate=startDate,endDate=endDate,field="tradeDate,accumAdjFactor")
            data_stk_n_avg_adj = pd.merge(data_stk_n_avg,df_adjfactor,left_index=True,right_on="tradeDate")    
            data_stk_n_avg_adj["adjtotalVolume"] = data_stk_n_avg_adj["totalVolume"]/data_stk_n_avg_adj["accumAdjFactor"]
            data_stk_n_avg_adj.index = data_stk_n_avg_adj['tradeDate']
            volume_ratio_df[stk] = data_stk_n_avg_adj['adjtotalVolume']
        except:
            wrong_list.append(stk)
        
    t2=time.time()
    print(t2-t1)
    return volume_ratio_df,wrong_list

startDate='20090101'
endDate='20160930'
path = "G:\\我的数据\\通联数据\\高频数据\\分钟数据\\data\\"

a,b=getVolumeRatio(startDate,endDate,path)

a.to_excel("D:\\百度云同步盘\\工作文档\\PYTHON\\intraday\\volumeratio.xlsx")