import pandas as pd
import json
import requests
import time
import os.path
import preprocess_top20 as preprocess

outputsize="full"
api_key = "BQLYAU0CSELLWVJ9"

def create_csv_data():
    df = pd.read_csv("top20companises_sectorwise.csv")
    companies_list = list(df["company_symbol"])
    sector_list = list(df["Sector"])
    for i in range(len(companies_list)):
        company_name = companies_list[i]
        sector_name = sector_list[i]
        print("This is the data of {} company of {} sector".format(company_name, sector_name))
        time.sleep(20)
        url="https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={}&outputsize={}&interval=1min&apikey={}".format(company_name,outputsize,api_key)
        req=requests.get(url)

        # if request success
        if req.status_code==200:
            raw_data=json.loads(req.content)
        try:
            price=raw_data['Time Series (Daily)']
            meta=raw_data['Meta Data']
        except KeyError:
            print(raw_data)
            print("we hit key error", req.status_code)
            exit()

        #print(raw_data)
        
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
            v.update({'symbol':company_name,'time':k})
        price=dict((key,dict((rename[k],v) for (k,v) in value.items())) for (key, value) in price.items())
        price=list(price.values())
        data_frame = pd.DataFrame(price)
        data_frame["sector"] = sector_name 
        data_frame = data_frame[data_frame["time"] > "2018-04-01"]
        data_frame = data_frame.sort_values("time")
        # path while calling this function from data_operations
        if  os.path.isfile('../../Data-Acquisition/ohlcv-data/ohlcv.csv') == False: 
            data_frame.to_csv('../../Data-Acquisition/ohlcv-data/ohlcv.csv', header=True)
        else:
            data_frame.to_csv('../../Data-Acquisition/ohlcv-data/ohlcv.csv', mode='a', header=False)
    return

def generate_ohlcv_data():
    preprocess.create_sector_csv()
    create_csv_data()
    # path while calling this function from data_operations
    df = pd.read_csv("../../Data-Acquisition/ohlcv-data/ohlcv.csv")
    return df
