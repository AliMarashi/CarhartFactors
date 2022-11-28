''''''

import pandas as pd
import numpy as np
import os

###
BalanceSheet = pd.read_excel('/content/drive/MyDrive/TheBestIdea/V4/Cods/4  Factors/balance sheet - 9811-bestidea.xlsx')
BalanceSheet.columns = ['name', 'date', 'bookvalue']
BalanceSheet['date'] = BalanceSheet['date'].str.replace("/" , "")
BalanceSheet['date'] = BalanceSheet.date.apply(lambda x: str(x)[0 :6])
BalanceSheet = BalanceSheet.drop_duplicates(['name'] , keep= 'last')
pd.set_option("max_rows", None)
BookVlue = BalanceSheet[['name', 'bookvalue']]

BookVlue['name'] = BookVlue['name'].str.replace(" " , '')
BookVlue['name'] = BookVlue['name'].str.replace("í" , '?')
BookVlue['name'] = BookVlue['name'].str.replace("ß" , '˜')
BookVlue['name'] = BookVlue['name'].str.replace("Â" , 'Ç')
BookVlue['name'] = BookVlue['name'].str.replace("Ã" , 'Ç')
###

#readig the data
StockPrice = pd.read_parquet('/content/drive/MyDrive/TheBestIdea/V4/Data/Cleaned_Stock_Prices_1400_06_29.parquet')

#data cleaning
StockPrice.drop(['date', 'max_price', 'min_price',
       'last_price', 'open_price', 'yesterday_price', 'value', 'volume',
       'quantity', 'stock_id', 'group_name', 'instId', 'baseVol',
       'title', 'max_price_Adjusted', 'min_price_Adjusted',
       'open_price_Adjusted', 'last_price_Adjusted', 'close_price_Adjusted',
       'group_id', 'return'] , axis=1 , inplace=True)
StockPrice = StockPrice.dropna()


StockPrice['name'] = StockPrice['name'].str.replace(" " , '')
StockPrice['name'] = StockPrice['name'].str.replace("í" , '?')
StockPrice['name'] = StockPrice['name'].str.replace("ß" , '˜')
StockPrice['name'] = StockPrice['name'].str.replace("Â" , 'Ç')
StockPrice['name'] = StockPrice['name'].str.replace("Ã" , 'Ç')

#create "pricechange" column
StockPrice['date'] = StockPrice.jalaliDate.apply(lambda x: str(x)[0 :6])
StockPrice = StockPrice.drop_duplicates(['date', 'name'] , keep= 'last')
StockPrice['ret_monthly'] = StockPrice.close_price.pct_change()

#data cleaning
StockPrice.drop(['jalaliDate', 'shrout', 'close_price'] , axis=1 , inplace=True)
StockPrice = StockPrice.sort_values(by =['date'])
FactorData = pd.merge(StockPrice , BookVlue , how= 'left' , on= "name")
FactorData.dropna(inplace=True)

###
df = FactorData
l1 = []
l2 = []
l3 = []
l4 = []

for date in df['date'].unique() :
    df2 = df[df['date'] == date]
    df2['ali'] = df2[['MarketCap']].apply(lambda x : pd.qcut(x ,
                                                             q = [0 , 0.33 ,
                                                                  0.66 , 1.0] ,
                                                             labels = ['small' ,
                                                                       'medium' ,
                                                                       'big']) if not x.nunique() == 1 else 'low')
    big = df2.groupby(df2.ali).apply(lambda x : np.average(x.ret_monthly ,
                                                           weights = x.MarketCap))[
        0]
    small = df2.groupby(df2.ali).apply(lambda x : np.average(x.ret_monthly ,
                                                             weights = x.MarketCap))[
        2]
    smb = small - big

    df3 = df[df['date'] == date]
    df3['ali'] = df3[['bookvalue']].apply(lambda x : pd.qcut(x ,
                                                             q = [0 , 0.5 ,
                                                                  1.0] ,
                                                             labels = ['low' ,
                                                                       'high']) if not x.nunique() == 1 else 'low')
    high = df3.groupby(df3.ali).apply(lambda x : np.average(x.ret_monthly ,
                                                            weights = x.MarketCap))[
        1]
    low = df3.groupby(df3.ali).apply(lambda x : np.average(x.ret_monthly ,
                                                           weights = x.MarketCap))[
        0]
    hml = high - low

    df4 = df[df['date'] == date]
    df4['ali'] = df4[['ret_monthly']].apply(lambda x : pd.qcut(x ,
                                                               q = [0 , 0.5 ,
                                                                    1.0] ,
                                                               labels = [
                                                                       'loser' ,
                                                                       'wenner']) if not x.nunique() == 1 else 'low')
    wenner = df4.groupby(df4.ali).apply(lambda x : np.average(x.ret_monthly ,
                                                              weights = x.MarketCap))[
        1]
    loser = df4.groupby(df4.ali).apply(lambda x : np.average(x.ret_monthly ,
                                                             weights = x.MarketCap))[
        0]
    UMD = wenner - loser

    l1.append(date)
    l2.append(smb)
    l3.append(hml)
    l4.append(UMD)

factor = pd.DataFrame([l1 , l2 , l3 , l4])
factor = factor.T
factor.columns = ['Period' , 'SMB' , 'hml' , 'UMD']

factor = factor.reset_index(drop = True)
os.chdir("/content/drive/MyDrive/TheBestIdea/V4/Cods/4  Factors")
factor.to_excel('factor.xlsx')



