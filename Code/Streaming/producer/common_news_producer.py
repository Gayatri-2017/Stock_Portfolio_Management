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

def query(start_date, end_date, page, news, year=None, month=None, key=None):
        
        root = 'https://api.nytimes.com/svc/search/v2/articlesearch.json?api-key={}&begin_date={}&end_date={}&page={}&fq=news_desk.contains:("{}")'
        key = 'v9gJZYhGqTvAgpqfi4b6GxxtdiTJvxi8'

        url = root.format(key, start_date, end_date, page, news)
        r = requests.get(url)
        return r.json()


def get_news(symbol):
    
    page = 0
    total_pages = 1
    
    news_list = []
    try:
        while page <= total_pages:
            page += 1
            
            mydict = query(symbol=symbol, page=page)
            total_pages = math.floor((pyjq.all('.response .meta .hits', mydict)[0])/10)
            
            news_list.append(mydict)
            time.sleep(1)
    
    except TypeError:
        return news_list
    
    return news_list

def monthdelta(date, delta):
    date = datetime.strptime(date, '%Y%m%d').date()
    m, y = (date.month+delta) % 12, date.year + ((date.month)+delta-1) // 12
    if not m: 
        m = 12
    d = min(date.day, [31,29 if y%4==0 and not y%400==0 else 28,31,30,31,30,31,31,30,31,30,31][m-1])
    
    return date.replace(day=d,month=m, year=y)

def kafka_producer_common_news(producer):
    
    print("*"*10, "COMMON NEWS", "*"*10)
    
    page = -1
    total_pages = 0

    news_desk = ["Business Day", "Business", "Entrepreneurs","Financial", "Magazine", "Personal Investing", "Personal Tech", "Politics", "Retail", "Small Business", \
          "Sunday Business", "Technology", "Washington", "Week", "Your Money", "World"]

    for news in news_desk:

        print("*"*80)

        print('Fetching for the news :',news)
        page = -1
        total_pages = 0

        start_date = '20180101'
        end_date = '20200401'

        api = 'v9gJZYhGqTvAgpqfi4b6GxxtdiTJvxi8'
        while page <= total_pages and start_date < end_date:
            temp_end_date = "20200401"
            page += 1
            mydict = query(start_date, end_date, page, news)
            total_pages = pyjq.all('.response .meta .hits', mydict)[0]
            
            if total_pages:
                total_pages = math.floor(total_pages/10)
            
            else:
                total_pages = 0
            
            time.sleep(8)

            doc_lis = mydict["response"]["docs"]
            print('len(doc_lis)', len(doc_lis))
            for i, doc in enumerate(doc_lis):
                msg_str = "{}_{}||{}".format("Common", "Sector", str(doc))
                producer.send(topic='common_news', value=bytes(msg_str, 'utf-8'))
            
            while (total_pages > 200):
                print("Inside while, total_pages = ", total_pages)
                date_object = monthdelta(end_date, -1)
                temp_end_date = date_object.strftime("%Y%m%d")
                mydict = query(start_date, temp_end_date, page, news)
                total_pages = pyjq.all('.response .meta .hits', mydict)[0]
                
                if total_pages:
                    total_pages = math.floor(total_pages/10)
                
                else:
                    total_pages = 0 
                
                end_date = temp_end_date
                time.sleep(8)

                doc_lis = mydict["response"]["docs"]
                print('len(doc_lis)', len(doc_lis))
                
                for i, doc in enumerate(doc_lis):
                    
                    doc["symbol"] = "Common"
                    doc["sector"] = "Sector"

                    msg_str = "{}".format(str(doc))

                    producer.send(topic='common_news', value=bytes(msg_str, 'utf-8'))
            
            start_date = temp_end_date
            end_date = '20200401'
