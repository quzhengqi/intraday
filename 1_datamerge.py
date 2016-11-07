# -*- coding: utf-8 -*-
"""
Created on Mon Oct 17 09:33:57 2016

@author: Administrator
"""
from mfactors.BaseFunction import (
    Zscore,
    getTrdDateListDaily,
)
##时间是从20090101---20160831
import pandas as pd
from dateutil.parser import parse
import datetime
path = "D:\\百度云同步盘\\工作文档\\PYTHON\\intraday\\"

def compareDate(a,bench,n=90):
    """比较两个日期，他们的差是否超过了3个月。
    若超过3个月，则返回1
    若没有超过3个月，则返回0
    """
    a1 =parse(a) #时间偏早
    b1 =parse(bench) #当时的时间
    gap = b1-a1
    if gap>datetime.timedelta(n):
        return 1
    else:
        return 0
    

#1. 读取数据
volumeratio = pd.read_excel(path+'volumeratio.xlsx') #开盘量比,20090105
ma = pd.read_pickle("D:\\百度云同步盘\\工作文档\\PYTHON\\intraday\\ma.pkl")
swing = pd.read_pickle(path+'swing.pkl') #日振幅,20070104--20160831
turnover = pd.read_pickle(path+'turnover.pkl') #日流通换手率,20070104---20160831

ST = pd.read_csv(path+'ST.csv',encoding='GB18030') #历史上ST股票的列表,时间从20020104--20160831
stockpool = pd.read_csv(path+'stockpool.csv',encoding='GB18030')
ST['tradeDate'] = ST['tradeDate'].map(lambda x:x[:4]+x[5:7]+x[8:10])
ma['tradeDate'] = ma['tradeDate'].map(lambda x:x[:4]+x[5:7]+x[8:10])
#1.1 统一日期的格式。
volumeratio.index = volumeratio.index.map(lambda x:x[:4]+x[5:7]+x[8:10])
stockpool['endDate'] = stockpool['endDate'].map(lambda x:x[:4]+x[5:7]+x[8:10])
stockpool['listDate'] = stockpool['listDate'].map(lambda x:x[:4]+x[5:7]+x[8:10])


tradingDays = volumeratio.index.tolist()
#2. 合并数据，最终是panel,item是因子名称，major_axis是日期，minor_axis股票代码
data_merged = {}
for trd in tradingDays:
    print(trd)
    v1 = volumeratio.ix[trd]
    s1 = swing[:,trd,:]
    m1 = ma[ma['tradeDate']==trd]
    del m1['ticker']
    t1 = turnover['turnoverRate',trd,:]
    sp1 = stockpool[['listDate','secID']]
    s1['volumeratio'] = v1
    s1['turnoverRate'] = t1
    s1 = pd.merge(s1,sp1,left_on=['secID'],right_on=['secID'])
    s1 = pd.merge(s1,ma,left_on=['secID'],right_on=['secID'])
    s1 = s1.dropna()
    s1.index=s1['secID']
#    s1['ma_sign'] = list(map(lambda x,y,z:x>=y and y>=z,s1['MA5'],s1['MA10'],s1['MA20']))
#    s1['ma_sign'] = s1['ma_sign']+0
    data_merged[trd] = s1

data_merged_wp = pd.Panel(data_merged)
data_merged_wp = data_merged_wp.transpose(2,0,1)
#pd.to_pickle(data_merged_wp,path+'data_merged.pkl')

#3. 构建放量指标
#换手率放量：过去20日成交量均值，过去250日成交量均值
#振幅放量: 过去20日振幅均值，过去250日振幅均值
# 3.1 均值指标
data_merged_wp['turnover_rolling_mean5'] = data_merged_wp['turnoverRate',:,:].rolling(window=5).mean()
data_merged_wp['turnover_rolling_mean20'] = data_merged_wp['turnoverRate',:,:].rolling(window=20).mean()
data_merged_wp['turnover_rolling_mean250'] = data_merged_wp['turnoverRate',:,:].rolling(window=250).mean()
data_merged_wp['swing_rolling_mean5'] = data_merged_wp['swing',:,:].rolling(window=5).mean()
data_merged_wp['swing_rolling_mean20'] = data_merged_wp['swing',:,:].rolling(window=20).mean()
data_merged_wp['swing_rolling_mean250'] = data_merged_wp['swing',:,:].rolling(window=250).mean()
# 3.2 中位数指标
data_merged_wp['turnover_rolling_median5'] = data_merged_wp['turnoverRate',:,:].rolling(window=5).median()
data_merged_wp['turnover_rolling_median20'] = data_merged_wp['turnoverRate',:,:].rolling(window=20).median()
data_merged_wp['turnover_rolling_median250'] = data_merged_wp['turnoverRate',:,:].rolling(window=250).median()
data_merged_wp['swing_rolling_median5'] = data_merged_wp['swing',:,:].rolling(window=5).median()
data_merged_wp['swing_rolling_median20'] = data_merged_wp['swing',:,:].rolling(window=20).median()
data_merged_wp['swing_rolling_median250'] = data_merged_wp['swing',:,:].rolling(window=250).median()
# 3.3 标准差指标
data_merged_wp['turnover_rolling_std5'] = data_merged_wp['turnoverRate',:,:].rolling(window=5).std()
data_merged_wp['turnover_rolling_std20'] = data_merged_wp['turnoverRate',:,:].rolling(window=20).std()
data_merged_wp['turnover_rolling_std250'] = data_merged_wp['turnoverRate',:,:].rolling(window=250).std()
data_merged_wp['swing_rolling_std5'] = data_merged_wp['swing',:,:].rolling(window=5).std()
data_merged_wp['swing_rolling_std20'] = data_merged_wp['swing',:,:].rolling(window=20).std()
data_merged_wp['swing_rolling_std250'] = data_merged_wp['swing',:,:].rolling(window=250).std()

# 4.1 基于均值的指标
data_merged_wp['turnover_rolling_mean5_20'] = data_merged_wp['turnover_rolling_mean5']/data_merged_wp['turnover_rolling_mean20']
data_merged_wp['swing_rolling_mean5_20'] = data_merged_wp['swing_rolling_mean5']/data_merged_wp['swing_rolling_mean20']
data_merged_wp['turnover_rolling_mean20_250'] = data_merged_wp['turnover_rolling_mean20']/data_merged_wp['turnover_rolling_mean250']
data_merged_wp['swing_rolling_mean20_250'] = data_merged_wp['swing_rolling_mean20']/data_merged_wp['swing_rolling_mean250']

data_merged_wp['distance_turnover_short'] = (data_merged_wp['turnover_rolling_mean5']-data_merged_wp['turnover_rolling_mean20'])\
                                    /data_merged_wp['turnover_rolling_std20']
data_merged_wp['distance_swing_short'] = (data_merged_wp['swing_rolling_mean5']-data_merged_wp['swing_rolling_mean20'])\
                                    /data_merged_wp['swing_rolling_std20']
                                    
data_merged_wp['distance_turnover_long'] = (data_merged_wp['turnover_rolling_mean20']-data_merged_wp['turnover_rolling_mean250'])\
                                    /data_merged_wp['turnover_rolling_std250']
data_merged_wp['distance_swing_long'] = (data_merged_wp['swing_rolling_mean20']-data_merged_wp['swing_rolling_mean250'])\
                                    /data_merged_wp['swing_rolling_std250']         
                
# 4.2 基于中位数的指标
data_merged_wp['turnover_rolling_median5_20'] = data_merged_wp['turnover_rolling_median5']/data_merged_wp['turnover_rolling_median20']
data_merged_wp['swing_rolling_median5_20'] = data_merged_wp['swing_rolling_median5']/data_merged_wp['swing_rolling_median20']
data_merged_wp['turnover_rolling_median20_250'] = data_merged_wp['turnover_rolling_median20']/data_merged_wp['turnover_rolling_median250']
data_merged_wp['swing_rolling_median20_250'] = data_merged_wp['swing_rolling_median20']/data_merged_wp['swing_rolling_median250']

data_merged_wp['distance_turnover_long_median'] = (data_merged_wp['turnover_rolling_median20']-data_merged_wp['turnover_rolling_median250'])\
                                    /data_merged_wp['turnover_rolling_std250']
data_merged_wp['distance_swing_long_median'] = (data_merged_wp['swing_rolling_median20']-data_merged_wp['swing_rolling_median250'])\
                                    /data_merged_wp['swing_rolling_std250']  


data_merged_wp['distance_turnover_short_median'] = (data_merged_wp['turnover_rolling_median5']-data_merged_wp['turnover_rolling_median20'])\
                                    /data_merged_wp['turnover_rolling_std20']
data_merged_wp['distance_swing_short_median'] = (data_merged_wp['swing_rolling_median5']-data_merged_wp['swing_rolling_median20'])\
                                    /data_merged_wp['swing_rolling_std20'] 




#pd.to_pickle(data_merged_wp,path+'data_merged.pkl')

#4. 对股票进行筛选
tradingDays1 =getTrdDateListDaily('20090105','20160930')
score={}
for trd in tradingDays1:
    if tradingDays1.index(trd)==0:
        continue
    else:
        pretrd = tradingDays1[tradingDays1.index(trd)-1]
        if parse(pretrd).month == parse(trd).month:
            #若不在当月。
            continue
        else:
            print(trd)
            #4.1剔除ST股票
            ST_list_temp = ST.where(ST['tradeDate']==trd).dropna()['secID'].tolist()
            stockpool_temp = stockpool[~stockpool['secID'].isin(ST_list_temp)]
            #4.2 剔除上市不满3个月的新股
            stockpool_temp['listdays'] = stockpool_temp['listDate'].map(lambda x:compareDate(x,trd))
            stockpool_temp = stockpool_temp[stockpool_temp['listdays']==1]
            stocklist = stockpool_temp['secID'].tolist()
            #注意！！！提取的数据是上个交易日的数据！
            data_temp = data_merged_wp[:,pretrd,stocklist].dropna()
            
            #4.3 剔除上天不交易的股票
            data_temp = data_temp[data_temp['isOpen']==1]
            data_temp['distance_turnover_long_median_r_s'] = Zscore(data_temp['distance_turnover_long_median'].rank())
            data_temp['distance_swing_long_median_r_s'] = Zscore(data_temp['distance_swing_long_median'].rank())
            data_temp['volumeratio_r_s'] = Zscore(data_temp['volumeratio'].rank())
            data_temp['distance_turnover_short_median_r_s'] = Zscore(data_temp['distance_turnover_short_median'].rank())
            data_temp['distance_swing_short_median_r_s'] = Zscore(data_temp['distance_swing_short_median'].rank())
            if len(data_temp)!=0:
                score[trd] = data_temp
score = pd.Panel(score)
score = score.transpose(2,0,1)
pd.to_pickle(score,path+'score.pkl')

        
        
        
        
        
        
        