# -*- coding: utf-8 -*-
"""
Created on Mon Oct 17 15:30:22 2016

@author: Administrator
"""

import pandas as pd
import numpy as np
from WindPy import w
w.start()
from dateutil.parser import parse

from mfactors.BaseFunction import (
    getTrdDateListWeekly,
    getTrdDateListDaily,
    getTrdDateList,
    Zscore,
    Winsorize,
    Ticker2WindCode,
)

from mfactors.PortForm import PortfolioForm
from mfactors.dataGit import dataGit
from mfactors.MF import MF
from mfactors.risk import RiskMetrics


import statsmodels.api as sm

    
def initialize(path=None):
    if path is None:
        dg = dataGit()
    else:
        dg=dataGit(path=path)
    wp=pd.read_pickle("D:\\百度云同步盘\\工作文档\\PYTHON\\intraday\\score.pkl")
    wp.minor_axis = wp.minor_axis.map(lambda x:Ticker2WindCode(x[:6]))
    wp1 = dg.getData_pickle()
#    wp1 = wp1[['Status','maxupordown'],wp.major_axis,wp.minor_axis]
    wp = pd.concat([wp1,wp],axis=0)
    ma = pd.read_pickle("D:\\百度云同步盘\\工作文档\\PYTHON\\intraday\\ma.pkl")
#    ma['tradeDate'] = ma['tradeDate'].map(lambda x:x[:4]+x[5:7]+x[8:10])
    ma['secID'] = ma['ticker'].map(lambda x:Ticker2WindCode(x[:6]))
    monthly_rate=dg.getReturn_pickle(freq='M')  #读取收益率数据
    monthly_benchrate=dg.getBenchReturn_pickle(freq='M')  #读取基准指数的收益率数据
    ma_shift = ma.groupby('ticker').shift(1) #注意使用的是上个交易日的数据！
    Data={
        'data':wp,
        'nextRate':monthly_rate,
        'benchRate':monthly_benchrate,
        'ma' :ma_shift
    }
    return Data


def blotter2daily(TimeBegin,TimeEnd,blotter):
    tradingDays_daily=getTrdDateListDaily(TimeBegin,TimeEnd)

    blotter_ext={}
    for i in range(len(tradingDays_daily)):
        date = tradingDays_daily[i] 
        if i==0:
            day=date
        else:
            predate=tradingDays_daily[i-1]
            if parse(date).month!=parse(predate).month:
                day=date
        blotter_ext[date]=blotter[day]
    
    return blotter_ext


def handle_data_daily(data,varDict,TimeBegin,TimeEnd,benchmark_name='000300.SH',num=150,fw_list=None):
    varList=[]
    for i in varDict.keys():
        varList.extend(varDict[i])

    # 每月调仓一次，根据因子的权重。若因子权重不变则不调仓。
    tradingDays=getTrdDateList(TimeBegin,TimeEnd)
    tradingDays_daily=getTrdDateListDaily(TimeBegin,TimeEnd)
    
    # 根据权重生成多因子，并构造组合
    mf_list,mf_list_temp={},{}
    #这里的性能不好可能原因是一直append.
    j=0

    for date in tradingDays:
        if j%20==0:
            mf_list.update(mf_list_temp)
            mf_list_temp={}
            
        if tradingDays.index(date)<12:
            tb = TimeBegin
        else:
            tb = tradingDays[tradingDays.index(date)-12]
#            tb = TimeBegin
        print(str(date)+str(tb))
        mf_boll=MF(tb,date,varList,freq='M',data=data,method='Equal-Weighted') #这里的频率参数要修改一下Customized
        mf=mf_boll.getWeight() #factor_weight=fw
        mf_list_temp[date]=mf['MF'].ix[date]
        j+=1
        
    mf_list.update(mf_list_temp)
    mf_list_temp={}
    
    blotter,blotter_mf={},{}
    dg=dataGit()
    data_monthly=data['data']
    ma = data['ma']

    for date in mf_list.keys():
        #筛选数据
        filte = data_monthly[['Status','maxupordown'],date,:]
        ma1 = ma[ma['tradeDate']==date].dropna()
        ma1['ma_sign'] = list(map(lambda x,y,z:((x>=y) and( y>=z))+0,ma1['MA5'],ma1['MA10'],ma1['MA20']))
        filte_ma = ma1[ma1['ma_sign']==1]['secID'].tolist()
        filte = filte.ix[filte_ma]  #找出那些多头排列的股票
        status_list = filte[filte['Status']=='交易' ]
#        status_list = status_list[status_list['maxupordown']!=1]
        raw_data=mf_list[date].ix[status_list.index].to_dict() #考虑当日可以交易
        tr=num/len(raw_data)
    
        x=PortfolioForm(raw_data=raw_data,weight_type=0,target_date=date,top_ratio=tr,select_type=0) #top_num
        blotter[date]=x.SimpleGroupOnly
        stk = list(x.SimpleGroupOnly.keys())
        blotter_mf[date] =mf_list[date].ix[stk]
    blotter_ext=blotter2daily(tradingDays[0],TimeEnd,blotter)
    
    #订单信号变成dataframe格式
    blotter_ext_df=pd.DataFrame(blotter_ext).fillna(0)
    blotter_ext_df=blotter_ext_df.T
    
    ret_daily=dg.getReturn_pickle(freq='D')
    bench_daily=dg.getBenchReturn_pickle(freq='D').drop_duplicates()
    ty_ext=ret_daily.ix[blotter_ext_df.index,blotter_ext_df.columns]*blotter_ext_df
    
    portRet_ext=ty_ext.sum(axis=1)
    pnl_df1 = pd.DataFrame(index = portRet_ext.index,columns=['portRate','portNv','benchmark']) 
    pnl_df1['portRate']=portRet_ext
    pnl_df1['benchmark']=bench_daily.ix[tradingDays_daily][benchmark_name]
    pnl_df1 = pnl_df1[pnl_df1.portRate!=0]
    pnl_df1['extRate']=pnl_df1['portRate']-pnl_df1['benchmark']
    pnl_df1['portNv'] = (1 + portRet_ext/100).cumprod()
    pnl_df1['extNv'] =(1+np.array(pnl_df1['extRate'])/140).cumprod()
    pnl_df1=pnl_df1.ix[tradingDays_daily]
    pnl_df1 = pnl_df1.dropna()
    date=pd.Series(pnl_df1.index.tolist()).map(lambda x:str(x))
    
    ret=RiskMetrics(date,algorithm_return=np.array(pnl_df1['extRate']), benchmark_return=np.array(pnl_df1['benchmark']))
    ret.update()
    ret.to_dict()
    risk_matrix=ret.to_dict()
    
    ret=RiskMetrics(date,algorithm_return=np.array(pnl_df1['portRate']), benchmark_return=np.array(pnl_df1['benchmark']))
    ret.update()
    ret.to_dict()
    risk_matrix1=ret.to_dict()
    #生成统计结果
    res={}
    res['pnl']=pnl_df1
    res['report_extRate']=risk_matrix
    res['report_portRate']=risk_matrix1
    res['method']='Fama-Macbeth'
    res['TimeBegin']=TimeBegin
    res['TimeEnd']=TimeEnd
    res['benchmark']=benchmark_name
    res['blotter']=blotter
    res['blotter_mf'] = blotter_mf
    res['varDict']=varDict
    
    num_days=len(tradingDays_daily)
    np.power(0.639,250/num_days)-1
    return res
 
def main():

    TimeBegin="20100201"  #"20090301""20100201"
    TimeEnd="20160930"
    
    varDict={}
    varDict['Ana_list']=["MV_S",'distance_turnover_long_median_r_s','distance_swing_long_median_r_s','volumeratio_r_s'] 
    
    alpha_list =[]
    for i in varDict.keys():
        alpha_list.extend(varDict[i])
        
    data=initialize()
#    wp = data['data']
    res_long = handle_data_daily(data,varDict,TimeBegin,TimeEnd,benchmark_name='000300.SH')
    blotter = res['blotter_mf']
    b1 = pd.Series(blotter['20160701'])
    return res
    
    
def monitor():
    data=initialize()

    varDict={}
    varDict['Ana_list']=['Adj_FY12P_S'] #分析师因子
    varDict['E_list']=["Adj_ROE_S"] #盈利类因子
    varDict['G_list']=["Adj_OperPG_S"] #成长类因子
    varDict['R_list']=["IVOL_S_S","Adj_TURN_S",'Adj_BETA_S',"AMIVEST_S"] #风险类因子
    varDict['S_list']=["MV_S"] #规模类因子
    varDict['T_list']=["Adj_REV_S"] #技术类因子
    varDict['V_list']=['Adj_PB_S'] #价值类因子

    varList=[]
    for i in varDict.keys():
        varList.extend(varDict[i])
        
    TimeBegin="20150801" #fama-macbeth回归的起点
    date='20160801'      #所用月份
    path="F:\\hs3.0\\"
    mf_list={}#多因子组合

    mf_boll=MF(TimeBegin,date,varList,freq='M',data=data,method='Fama-Macbeth') #这里的频率参数要修改一下Customized
    mf=mf_boll.getWeight() #factor_weight=fw
    mf_list[date]=mf['MF'].ix[date]
    
    blotter={}
    dg = dataGit()
    data_monthly=dg.getData_pickle()
    filte = data_monthly[['Status'],date,:]
    status_list = filte[filte['Status']=='交易' ]
        
    raw_data=mf_list[date].ix[status_list.index].to_dict() #考虑当日可以交易
    x =PortfolioForm (raw_data=raw_data,weight_type=0,target_date=date,top_ratio=0.05,select_type=0)
    blotter[date]=x.SimpleGroupOnly
        
    blotter=pd.DataFrame(blotter).fillna(0)
    blotter.to_excel(path+date+'monthly2.xlsx')