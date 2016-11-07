# -*- coding: utf-8 -*-
from dataapi import Client
if __name__ == "__main__":
    try:
        client = Client()
        client.init('e97fd48ed3ada633e20848c501fa018db3a52734767bfedc20ce0f1ac3aea723')
        
        
        url1='/api/macro/getChinaDataGDP.json?field=&indicID=M010000002&indicName=&beginDate=&endDate='
        code, result = client.getData(url1)
        if code==200:
            print (result)
        else:
            print (code)
            print (result)
        url2='/api/subject/getThemesContent.json?field=&themeID=&themeName=&isMain=1&themeSource='
        code, result = client.getData(url2)
        if(code==200):
            file_object = open('thefile.csv', 'w')
            file_object.write(result)
            file_object.close( )
        else:
            print (code)
            print (result)
            
            
        url3 = '/api/equity/getEqu.json?field=&ticker=&secID=&equTypeCD=A&listStatusCD=L'
        code, result = client.getData(url3)
        
        url4 = '/api/market/getMktStockFactorsOneDayPro.json?field=ticker,tradeDate,pe&secID=&ticker=000001,600000&tradeDate=20160727'
        code,result = client.getData(url4)
        
        url5 = '/api/equity/getSecST.json?field=&secID=&ticker=000521&beginDate=20020101&endDate=20160831'
        code,result_st = client.getData(url5)
    except Exception as e:
        #traceback.print_exc()
        raise e



result.to_csv('stockpool.csv')