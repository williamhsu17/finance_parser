# -*- coding: utf-8 -*-
import io
import pandas as pd
from IPython.display import display_html

import requests
import json

import pymysql

#----------------------------------------------------------------------------------------------
# this part take response text as input and parse for given field in 大盤統計資訊 at given date
# see 'fields' for fields used
#
#----------------------------------------------------------------------------------------------
def parse_market_info(data):
    data_market_info = pd.DataFrame({'date': data['date']},index=[0])
    
    used_key = 'data7'
    
    # 使用中的大盤統計資訊欄位
    fields = ['一般股票', 
              '台灣存託憑證', 
              '受益憑證', 
              'ETF', 
              '受益證券', 
              '變更交易股票', 
              '認購(售)權證', 
              '轉換公司債', 
              '附認股權特別股', 
              '附認股權公司債', 
              '認股權憑證', 
              '公司債', 
              'ETN',
              '證券合計(1+6)',
              '總計(1~13)']
    
    # 取出 table:大盤統計資訊值
    for n, field in enumerate(fields):
        if field in data[used_key][n][0]:
            data_market_info[f'{field}_成交金額(元)'] = str(data[used_key][n][1])
            data_market_info[f'{field}_成交股數(股)'] = str(data[used_key][n][2])
            data_market_info[f'{field}_成交筆數'] = str(data[used_key][n][3])
        else:
            print("Error: can't find field")
    
    return data_market_info

#----------------------------------------------------------------------------------------------
# this part take response text as input and parse for given field in 漲跌證券數合計 at given date
# see 'fields' for fields used
#
#----------------------------------------------------------------------------------------------
def parse_upsdown(data):
    data_upsdown = pd.DataFrame({'date': data['date']},index=[0])
    
    used_key = 'data8'
    
    # 使用中的漲跌證券數
    fields = ['上漲(漲停)', 
              '下跌(跌停)', 
              '持平', 
              '未成交', 
              '無比價']
    
    # 取出 table:漲跌證券數合計
    for n, field in enumerate(fields):
        if field in data[used_key][n][0]:
            data_upsdown[f'{field}_整體市場'] = str(data[used_key][n][1])
            data_upsdown[f'{field}_股票'] = str(data[used_key][n][2])
        else:
            print("Error: can't find field")
    
    return data_upsdown

#----------------------------------------------------------------------------------------------
# main section
#
#----------------------------------------------------------------------------------------------
def main():
    date = '20200227'
    
    url = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date=' + date
    print(url)
    response = requests.get(url)
    
    data_raw = json.loads(response.text)
    
    data_market_info = parse_market_info(data_raw)
    data_upsdown = parse_upsdown(data_raw)
    
    data = pd.merge(data_market_info, data_upsdown, on='date', how='outer')
    
    db = pymysql.connect("localhost","root","password","stock_table")
    cursor = db.cursor()
    
    for index, row in data.iterrows():
        print(index)
        
        sql = ''
        print(sql)
        cursor.execute(sql)
        db.commit()
    return data
    
if __name__ == "__main__":
    main()