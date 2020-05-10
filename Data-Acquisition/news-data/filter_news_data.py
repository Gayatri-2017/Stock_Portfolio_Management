import json
import pyjq
import pandas as pd
import os

# parse file
def parse_news(data, ticker, sector):
	json_data = json.loads(data)

	jq_query = f'.response .docs [] | {{snippet: .snippet, headline: .headline .main, \
	date: .pub_date, news_desk: .news_desk, keywords: .keywords | [.[] | .value],\
	leading_paragraph: .lead_paragraph, abstract: .abstract}} '

	output = pyjq.all(jq_query, json_data)
	news_df = pd.DataFrame(output) 
	
	if(not news_df.empty):

		news_df["date"] = pd.to_datetime(news_df["date"]).dt.date

		news_df = news_df.fillna(" ")

		news_df = news_df.groupby(['date']).agg({'snippet': ' '.join, 'headline': ' '.join, \
			'news_desk': ' '.join, "keywords": "sum", "leading_paragraph": ' '.join, \
			"abstract": ' '.join}).reset_index()
		news_df["symbol"] = ticker
		news_df["sector_name"] = sector
		
		news_df['combined_data'] = news_df[['snippet', 'headline', 'leading_paragraph', 'abstract']].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)

		return news_df

def read_files(mypath):
	
	cols = ['snippet', 'headline', 'date', 'news_desk','keywords', 'leading_paragraph', 'abstract', 'symbol', 'sector_name', 'combined_data']

	complete_df = pd.DataFrame(columns=cols)

	directory_list = [x[1] for x in os.walk(mypath)][0]

	for dir1 in directory_list:
		ticker, sector = dir1.split("_")
		print("ticker = ",ticker, "sector = ", sector)

		file_name = mypath+dir1
		onlyfiles = [f for f in os.listdir(file_name) if os.path.isfile(os.path.join(file_name, f))]
		for file in onlyfiles:
			f_name = os.path.join(file_name, file)
			print(f_name)
			with open(f_name) as myfile:
			    data=myfile.read()
			    news_df = parse_news(data, ticker, sector)
			    complete_df = complete_df.append(news_df, ignore_index = True, sort=False)

	complete_df = complete_df[cols]
	
	complete_df = complete_df.groupby(['date', "symbol", "sector_name"]).agg({'snippet': ' '.join, 'headline': ' '.join, \
			'news_desk': ' '.join, "keywords": "sum", "leading_paragraph": ' '.join, \
			"abstract": ' '.join, "combined_data": ' '.join}).reset_index()

	complete_df = complete_df.sort_values(by="date")
	complete_df = complete_df[complete_df["date"]>=pd.to_datetime('2018-04-01')]


	return complete_df
