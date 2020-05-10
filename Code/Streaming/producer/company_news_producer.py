import json
import requests
import schedule
from pytz import timezone    
from kafka import KafkaProducer
import pyjq
import math
import time
import pandas as pd
from datetime import datetime

def query(company_name, page, year=None, month=None, key=None):
   
    root = 'https://api.nytimes.com/svc/search/v2/articlesearch.json?api-key={}&begin_date={}&end_date={}&page={}&fq=organizations:("{}")'
    key = 'v9gJZYhGqTvAgpqfi4b6GxxtdiTJvxi8'

    url = root.format(key, '20180401', '20200331', page, company_name)
    print("url = \n", url)
    r = requests.get(url)
    return r.json()

def kafka_producer_company_news(producer, row_id=0):

    print("*"*10, "COMPANY NEWS", "*"*10)
    api = 'v9gJZYhGqTvAgpqfi4b6GxxtdiTJvxi8'

    reader_df = pd.read_csv('../../data_operations/CompaniesList.csv', header=0)
    print(reader_df.head())
    page = -1
    total_pages=0
    for row_i in range(row_id, len(reader_df)): 
        try:
            print("*"*80)
            row = reader_df.iloc[row_i]
            row_id = row_i
            print("row = \n", row)
            
            ticker = row["company_symbol"]
            company_name = row["Name"].replace('&', '%26')
            sector = row["Sector"]
            
            page = -1
            total_pages=0
            while page <= total_pages:
                page += 1
                mydict = query(company_name, page)
                time.sleep(10)
            
                total_pages = math.floor((pyjq.all('.response .meta .hits', mydict)[0])/10)
                print("total_pages = ", total_pages)

                doc_lis = mydict["response"]["docs"]
                print('len(doc_lis)', len(doc_lis))

                for i, doc in enumerate(doc_lis):
                    doc["symbol"] = ticker
                    doc["sector"] = sector

                    msg_str = "{}".format(str(doc))

                    producer.send(topic='company_news', value=bytes(msg_str, 'utf-8'))

        except TypeError as e:
            kafka_producer_company_news(producer, row_id)
