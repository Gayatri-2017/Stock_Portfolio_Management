import json
import requests
import schedule
import numpy as np
from kafka import KafkaProducer
from newsapi.newsapi_client import NewsApiClient
import pandas as pd
import time

def ohlcv_data(symbol,outputsize='full'):
    
    api_key = "0MCP1QDS7AXSPLUU"
    url="https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={}&outputsize={}&interval=1min&apikey={}".format(symbol,outputsize,api_key)

    req=requests.get(url)
    time.sleep(20)
    if req.status_code==200:
        raw_data=json.loads(req.content)
        try:
            price=raw_data['Time Series (Daily)']

        except KeyError:
            print(raw_data)
            exit()
    
        rename={'symbol':'symbol',
                'time':'time',
                '1. open':'open',
                '2. high':'high',
                '3. low':'low',
                '4. close':'close',
                '5. adjusted close':'adjusted_close',
                '6. volume':'volume',
                '7. dividend amount':'dividend_amount',
                '8. split coefficient':'split_coefficient'}
        
        for k,v in price.items():
            v.update({'symbol':symbol,'time':k})
        price=dict((key,dict((rename[k],v) for (k,v) in value.items())) for (key, value) in price.items())
        price=list(price.values())
        
        return price

def ohlcv_kafka_producer(producer):
    print("*"*10, "OHLCV DATA", "*"*10)

    company_list_df = pd.read_csv("CompaniesList.csv")
    companies_list = list(company_list_df["company_symbol"])
    for symbol in companies_list:
        value = ohlcv_data(symbol)
        producer.send(topic='stock_streaming1', value=bytes(str(value), 'utf-8'))
