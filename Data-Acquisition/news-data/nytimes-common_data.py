import sys, csv, json
import time
import pyjq
import os, math
from datetime import datetime

key = 'v9gJZYhGqTvAgpqfi4b6GxxtdiTJvxi8'      #article search api key

import requests
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
        self.key = 'v9gJZYhGqTvAgpqfi4b6GxxtdiTJvxi8'
        
        self.root = 'https://api.nytimes.com/svc/search/v2/articlesearch.json?api-key={}&begin_date={}&end_date={}&page={}&fq=news_desk.contains:(\"{}\")'
        if not self.key:
            nyt_dev_page = 'http://developer.nytimes.com/docs/reference/keys'
            exception_str = 'Warning: API Key required. Please visit {}'
            raise NoAPIKeyException(exception_str.format(nyt_dev_page))

    def query(self, start_date, end_date, page, news, year=None, month=None, key=None, ):
        """
        Calls the archive API and returns the results as a dictionary.
        :param key: Defaults to the API key used to initialize the ArchiveAPI class.
        """
        if not key: key = self.key
        url = self.root.format(key, start_date, end_date, page, news)
        r = requests.get(url)
        return r.json()

# Replace below key with your NYTimes Developer key

def monthdelta(date, delta):
	date = datetime.strptime(date, '%Y%m%d').date()
	m, y = (date.month+delta) % 12, date.year + ((date.month)+delta-1) // 12
	if not m: m = 12
	d = min(date.day, [31,29 if y%4==0 and not y%400==0 else 28,31,30,31,30,31,31,30,31,30,31][m-1])
	return date.replace(day=d,month=m, year=y)

page = -1
total_pages = 0
mypath = "./Common_news/"
start_date = '20180101'
end_date = '20200401'
news_desk = ["Business Day", "Business", "Entrepreneurs","Financial", "Magazine", "Personal Investing", "Personal Tech", "Politics", "Retail", "Small Business", \
			"Sunday Business", "Technology", "Washington", "Week", "Your Money", "World"]
for news in news_desk:
	print('Fetching for the news :',news)
	page = -1
	total_pages = 0
	start_date = '20180101'
	end_date = '20200401'
	api = ArchiveAPI('key')
	while page <= total_pages and start_date < end_date:
		temp_end_date = '20200401'
		page += 1
		file_str = mypath+news+"{:04}".format(page)+".json"
		mydict = api.query(start_date, end_date, page, news)
		total_pages = pyjq.all('.response .meta .hits', mydict)[0]
		if total_pages:
			total_pages = math.floor(total_pages/10)
		else:
			total_pages = 0
		time.sleep(8)
		while (total_pages > 200):
			date_object = monthdelta(end_date, -1)
			temp_end_date = date_object.strftime("%Y%m%d")
			mydict = api.query(start_date, temp_end_date, page, news)
			total_pages = pyjq.all('.response .meta .hits', mydict)[0]
			if total_pages:
				total_pages = math.floor(total_pages/10)
			else:
				total_pages = 0 
			end_date = temp_end_date
			time.sleep(8)

		with open(file_str, 'w') as fout:
			json.dump(mydict, fout)
			fout.close()
		start_date = temp_end_date
		end_date = '20200401'