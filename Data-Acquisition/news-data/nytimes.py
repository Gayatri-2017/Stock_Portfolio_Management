#!/usr/bin/env python2
"""
"""
# Below part is not required
########## News API ########################################################
# key = 'JwLKzlcdwaGqOXqeiGzZGSiOmO8WN2Oi'
# params = {}
# api = NewsAPI(key)
# sources = api.sources(params)
# articles = api.articles(sources[0]['id'], params)

################ NY Times API #############################################

import sys, csv, json
import time
import pyjq
import os, math

key = 'GiDQSAv4b1T8LP6A3TQixHPibZtrhB0x'      #article search api key
import requests
import pandas as pd
"""
About:
Python wrapper for the New York Times Archive API 
https://developer.nytimes.com/article_search_v2.json
"""

class APIKeyException(Exception):
    def __init__(self, message): self.message = message 

class InvalidQueryException(Exception):
    def __init__(self, message): self.message = message 

class ArchiveAPI(object):
    def __init__(self, key=None):
        """
        Initializes the ArchiveAPI class. Raises an exception if no API key is given.
        :param key: New York Times API Key
        """
        self.key = 'GiDQSAv4b1T8LP6A3TQixHPibZtrhB0x'
        self.root = 'https://api.nytimes.com/svc/search/v2/articlesearch.json?api-key={}&begin_date={}&end_date={}&page={}&fq=organizations:(\"{}\")'

        if not self.key:
            nyt_dev_page = 'http://developer.nytimes.com/docs/reference/keys'
            exception_str = 'Warning: API Key required. Please visit {}'
            raise NoAPIKeyException(exception_str.format(nyt_dev_page))

    def query(self, company_name, page, year=None, month=None, key=None):
        """
        Calls the archive API and returns the results as a dictionary.
        :param key: Defaults to the API key used to initialize the ArchiveAPI class.
        """
        if not key: 
            key = self.key
        
        url = self.root.format(key, '20180401', '20200331', page, company_name)
        r = requests.get(url)
        return r.json()

def func_name(api, mypath, row_id=0):
    # reader_df = pd.read_csv('/Users/amoghkallihal/Documents/CMPT-733/Project/CompaniesList.csv', header=0)
    reader_df = pd.read_csv('CompaniesList.csv', header=0)
    print(reader_df.head())
    page = -1
    total_pages=0

    for row_i in range(row_id, len(reader_df)): 
        try:
            row = reader_df.iloc[row_i]
            row_id = row_i
            print("row = \n", row)
            
            ticker = row["company_symbol"]
            company_name = row["Name"].replace('&', '%26')
            sector = row["Sector"]
            path1 = mypath+ticker+'_'+sector
            if not os.path.exists(path1):
                os.mkdir(path1)  
            page = -1
            total_pages=0

            while page <= total_pages:
                page += 1
                file_str = mypath+ticker+'_'+sector+'/'+'{:04}'.format(page)+'.json'
                mydict = api.query(company_name, page)
                time.sleep(10)
            
                total_pages = math.floor((pyjq.all('.response .meta .hits', mydict)[0])/10)
                print(total_pages)
                with open(file_str, 'w') as fout:
                    json.dump(mydict, fout)
                fout.close()

        except TypeError as e:
            func_name(api, mypath, row_id)
                
   
# Replace below key with your NYTimes Developer key
api = ArchiveAPI('key')
mypath = "./Company_news/"

func_name(api, mypath, row_id=0)              




    
