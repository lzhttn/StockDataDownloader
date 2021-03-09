# -*- coding: utf-8 -*-
"""
Created on Wed Dec  9 00:07:00 2020

@author: L
"""

import baostock as bs
import pandas as pd
import time 
import os
#### 登陆系统 ####
lg = bs.login()
# 显示登陆返回信息
#print('login respond error_code:'+lg.error_code)
#print('login respond  error_msg:'+lg.error_msg)

PATH_DOWN_STOCK = r'C:\pv\baostock data\stock20210301'
PATH_DOWN_INDEX = r'C:\pv\baostock data\index20210301'

def to_df(rs):
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    return result

#info = to_df(info)


#### 获取沪深A股历史K线数据 ####
# 详细指标参数，参见“历史行情指标参数”章节；“分钟线”参数与“日线”参数不同。“分钟线”不包含指数。
# 分钟线指标：date,time,code,open,high,low,close,volume,amount,adjustflag
# 周月线指标：date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg

'''
rs = bs.query_history_k_data_plus("sh.600000",
    "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
    start_date='2011-12-21', end_date='2020-12-31',
    frequency="d", adjustflag="3")
# print('query_history_k_data_plus respond error_code:'+rs.error_code)
# print('query_history_k_data_plus respond  error_msg:'+rs.error_msg)

#### 打印结果集 ####
data_list = []
while (rs.error_code == '0') & rs.next():
    # 获取一条记录，将记录合并在一起
    data_list.append(rs.get_row_data())
result = pd.DataFrame(data_list, columns=rs.fields)

#### 结果集输出到csv文件 ####   
result.to_csv("D:\\history_A_stock_k_data.csv", index=False)
print(result)
'''

def down_index(code_lis):
    if not os.path.exists(PATH_DOWN_INDEX):
        os.makedirs(PATH_DOWN_INDEX)
    print("开始下载...")
    for code in code_lis:
        fn ="%s/%s.csv"%(PATH_DOWN_INDEX, code)
        if not os.path.exists(fn):
            # if code[:6] != 'sz.300':
            rs = bs.query_history_k_data_plus(code,
                "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
                start_date='2016-01-01', end_date='2021-12-31',
                frequency="d", adjustflag="2")#frequency="d"表示日K，5表示5分钟线， #adjustflag="2 表示前复权
            rs_df = to_df(rs)
            rs_df.to_csv(fn, index=None)
            time.sleep(0.01)
    return 

def down_stocks(code_lis):
    if not os.path.exists(PATH_DOWN_STOCK):
        os.makedirs(PATH_DOWN_STOCK)
    print("开始下载...")
    for code in code_lis:
        fn ="%s/%s.csv"%(PATH_DOWN_STOCK, code)
        if not os.path.exists(fn):
            # if code[:6] != 'sz.300':
            rs = bs.query_history_k_data_plus(code,
                "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
                start_date='2016-01-01', end_date='2021-12-31',#,peTTM,pbMRQ,psTTM,pcfNcfTTM
                frequency="d", adjustflag="2")#qfq 前复权
            rs_df = to_df(rs)
            rs_df.to_csv(fn, index=None)
            time.sleep(0.01)
    return 

def down_stocks_5min(code_lis):
    if not os.path.exists(PATH_DOWN_STOCK):
        os.makedirs(PATH_DOWN_STOCK)
    print("开始下载...")
    for code in code_lis:
        fn ="%s/%s.csv"%(PATH_DOWN_STOCK, code)
        if not os.path.exists(fn):
            # if code[:6] != 'sz.300':
            rs = bs.query_history_k_data_plus(code,
                "date,code,open,high,low,close,volume,amount,adjustflag",
                start_date='2016-01-01', end_date='2021-12-31',#,peTTM,pbMRQ,psTTM,pcfNcfTTM
                frequency="5", adjustflag="2")#qfq 前复权
            rs_df = to_df(rs)
            rs_df.to_csv(fn, index=None)
            time.sleep(0.01)
    return 

def down_hs300_stock():
    rs = bs.query_hs300_stocks()
    # print('query_hs300 error_code:'+rs.error_code)
    # print('query_hs300  error_msg:'+rs.error_msg)
    
    # 打印结果集
    hs300_stocks = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        hs300_stocks.append(rs.get_row_data())
    result = pd.DataFrame(hs300_stocks, columns=rs.fields)
    return result

info = to_df(bs.query_stock_basic())
stock_code_lis= info[ info['type']=='1']['code'].tolist()

# down_stocks_5min(stock_code_lis)
# down_stocks(stock_code_lis)    

hs300_lis = down_hs300_stock()
down_stocks( hs300_lis['code'].tolist() )

# down_index(['sh.000001'])