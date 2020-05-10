from cassandra.cluster import Cluster
from pyspark.sql.functions import desc, col
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession, functions
import pandas as pd
import talib as ta
import uuid
from datetime import datetime
# import input_features

from pyspark.sql import SQLContext

# Command to run the file (makes connection to cassandra database)

# spark-submit --packages datastax:spark-cassandra-connector:2.4.0-s_2.11 data_operations.py

import sys, os
sys.path.append(os.path.abspath(os.path.join('../../Data-Acquisition', 'news-data')))
import filter_news_data as fnd
sys.path.append(os.path.abspath(os.path.join('../../Data-Acquisition', 'ohlcv-data')))
import create_csv_top20 as cct


cluster_seeds = ['127.0.0.1']
spark = SparkSession.builder.appName('data operations').getOrCreate()
spark = SparkSession.builder.appName('Stock').config('spark.cassandra.connection.host',
                                                             ','.join(cluster_seeds)).getOrCreate()
sc = spark.sparkContext

KEYSPACE_NAME = 'stocks'

##########################################################################################
#####################################READING FROM DB######################################
##########################################################################################

def get_quarter_1_ohlc_data_from_db(tablename="ohlc_quarter_1", KEYSPACE=KEYSPACE_NAME):
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table=tablename, keyspace=KEYSPACE).load()
    return data.toPandas()

def get_all_ohlc_data_from_db(tablename="ohlc", KEYSPACE=KEYSPACE_NAME):
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table=tablename, keyspace=KEYSPACE).load()
    return data.toPandas()


def get_all_ohlcv_data_from_db(tablename="ohlcv", KEYSPACE=KEYSPACE_NAME):

    data = spark.read.format("org.apache.spark.sql.cassandra").options(table=tablename, keyspace=KEYSPACE).load()
    data_pd = data.toPandas()
    data_pd['time'] = pd.to_datetime(data_pd['time'], format='%d/%m/%Y')
    return data_pd

def get_pca_quarter_1_data_from_db(tablename="pca_quarter_1", KEYSPACE=KEYSPACE_NAME):
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table=tablename, keyspace=KEYSPACE).load()
    return data.toPandas()

def get_all_pca_data_from_db(tablename="all_pca", KEYSPACE=KEYSPACE_NAME):
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table=tablename, keyspace=KEYSPACE).load()
    return data.toPandas()

def get_common_news_from_db(tablename="common_news", KEYSPACE=KEYSPACE_NAME):
	data = spark.read.format("org.apache.spark.sql.cassandra").options(table=tablename, keyspace=KEYSPACE).load()
	return data

def get_company_news_from_db(tablename="company_news", KEYSPACE=KEYSPACE_NAME):
	data = spark.read.format("org.apache.spark.sql.cassandra").options(table=tablename, keyspace=KEYSPACE).load()
	return data

def get_news_from_db(tablename="news", KEYSPACE=KEYSPACE_NAME):
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table=tablename, keyspace=KEYSPACE).load()
    return data

def get_company_sentiment_data_from_db(tablename="company_sentiment", KEYSPACE=KEYSPACE_NAME):
	data = spark.read.format("org.apache.spark.sql.cassandra").options(table=tablename, keyspace=KEYSPACE).load()
	return data.toPandas()

def get_common_sentiment_data_from_db(tablename="common_sentiment", KEYSPACE=KEYSPACE_NAME):
	data = spark.read.format("org.apache.spark.sql.cassandra").options(table=tablename, keyspace=KEYSPACE).load()
	return data.toPandas()

def get_input_features_from_db(tablename="input_features", KEYSPACE=KEYSPACE_NAME):
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table=tablename, keyspace=KEYSPACE).load()
    return data.toPandas()

##########################################################################################
#####################################READING FROM CSV#####################################
##########################################################################################

def get_quarter_1_ohlc_data_from_csv(filename="ohlcvquarter_1.csv"):
    data = spark.read.format('csv').load(filename, header=True) 
    return data

def get_all_ohlc_data_from_csv(filename="ohlcv.csv"):
    data = spark.read.format('csv').load(filename, header=True) 
    return data

def get_all_ohlcv_data_from_csv(filename="ohlcv.csv"):
    data = spark.read.format('csv').load(filename, header=True) 
    return data

def get_common_news_data_from_csv(filename="news_common_till_april.csv"):
	data = spark.read.format('csv').load(filename, header=True) 
	return data

def get_company_news_data_from_csv(filename="news_company_till_april.csv"):
	data = spark.read.format('csv').load(filename, header=True) 
	return data

def get_quarter_1_sector_ohlc_median(tablename="ohlc_quarter_1", KEYSPACE=KEYSPACE_NAME):
	df = get_quarter_1_data_from_db(tablename, KEYSPACE=KEYSPACE_NAME)
	groupby_df = df.groupby("sector").median().reset_index()
	return groupby_df

def get_all_ohlc_time_median(df):
	df['time'] = df['time'].dt.date
	groupby_df = df.groupby("time").median().reset_index()
	return groupby_df

def get_all_features_time_median(df):
	df['time'] = df['time'].dt.date
	groupby_df = df.groupby("time").median().reset_index()
	return groupby_df

def get_top20companises_sectorwise_data_from_csv(filename="top20companises_sectorwise.csv"):
	data = spark.read.format('csv').load(filename, header=True) 
	return data

def get_pca_quarter_1_data_from_csv(filename="1_pca_qt_result.csv"):
	data = spark.read.format('csv').load(filename, header=True) 
	return data

def get_all_pca_data_from_csv(filename="./pca_folder/pca_complete_result.csv"):
	data = spark.read.format('csv').load(filename, header=True) 
	return data

def get_all_pca_with_sentiment_data_from_csv(folder_name="../k_means_cluster_analysis/Results_with_sentiment/pca_folder_cl3/"):

	# Returns a Pandas DF

	cols = ["PC_0", "PC_1", "PC_2", "PC_3", "PC_4", "PC_5", "PC_6", "PC_7", "symbol", "sector", "label", "Quarter"]
	complete_df = pd.DataFrame(columns=cols)
	for i in range(1, 9):
		file_name = folder_name + str(i) + "_pca_qt_result.csv"
		
		df = pd.read_csv(file_name)
		df["Quarter"] = i
		complete_df = complete_df.append(df, ignore_index = True, sort=False)

	complete_df = complete_df[cols]
	return complete_df

def get_all_pca_without_sentiment_data_from_csv(folder_name="../k_means_cluster_analysis/Results_without_sentiment/pca_folder_cl3/"):
	# Returns a Pandas DF

	cols = ["PC_0", "PC_1", "PC_2", "PC_3", "PC_4", "PC_5", "PC_6", "PC_7", "symbol", "sector", "label", "Quarter"]
	complete_df = pd.DataFrame(columns=cols)
	for i in range(1, 9):
		file_name = folder_name + str(i) + "_pca_qt_result.csv"
		
		df = pd.read_csv(file_name)
		df["Quarter"] = i
		complete_df = complete_df.append(df, ignore_index = True, sort=False)

	complete_df = complete_df[cols]
	return complete_df

def get_profile_with_sentiment_from_csv(folder_name="../k_means_cluster_analysis/Results_with_sentiment/Profiles_qtrwise_cl3/", time_period=3):
	# Returns a Pandas DF

    file_name = folder_name+str(time_period)+'_quarter_profile.csv'
    df = pd.read_csv(file_name)
    print("file_name = \n", file_name)
    print("Inside get_profile_with_sentiment_from_csv, df.columns = \n", df.columns)
    print("df = ", df)

    return df

def get_profile_without_sentiment_from_csv(folder_name="../k_means_cluster_analysis/Results_without_sentiment/Profiles_qtrwise_cl3/", time_period=3):
	# Returns a Pandas DF

	file_name = folder_name+str(time_period)+'_quarter_profile.csv'
	df = pd.read_csv(file_name)

	return df

def get_portfolio_generation_two_year_with_sentiment_from_csv(folder_name="../data_operations/profile_generation/"):
    
    file_name = folder_name + "two-year-with-senti.csv"
    df = pd.read_csv(file_name)
    return df

def get_combine_news_text_content_one_column():
    # read data from news table
    df = get_news_from_db(tablename="news", KEYSPACE=KEYSPACE_NAME)
    
    pd_df = df.select("*").toPandas()

    pd_df = pd_df.groupby(['date', "symbol", "sector_name"]).agg({'snippet': ' '.join, 'headline': ' '.join, \
            'news_desk': ' '.join, "keywords": "sum", "leading_paragraph": ' '.join, \
            "abstract": ' '.join}).reset_index()

    pd_df['combined_data'] = pd_df[['snippet', 'headline', 'leading_paragraph', 'abstract']].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)

    pd_df = pd_df[['date', "symbol", "sector_name", "combined_data"]]
    pd_df = pd_df.sort_values(by="date")
    pd_df = pd_df[pd_df["date"]>=pd.to_datetime('2018-04-01')]

    return pd_df

##########################################################################################
##########################CONNECT TO CASSANDRA AND CREATE TABLES##########################
##########################################################################################

def connect_to_cassandra(KEYSPACE=KEYSPACE_NAME):
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'} \
        AND durable_writes = 'true'" % KEYSPACE_NAME)
    session = cluster.connect(keyspace=KEYSPACE)
    return session

def create_cassandra_tables(session):

    session.execute('CREATE TABLE IF not EXISTS ohlcv \
    	(symbol TEXT, \
    	sector TEXT,\
    	open float,\
    	high float,\
    	low float,\
    	close float,\
    	volume float,\
    	adjusted_close float,\
    	dividend_amount float,\
    	split_coefficient float,\
    	time timestamp,\
    	PRIMARY KEY (time, symbol))')

    session.execute('CREATE TABLE IF not EXISTS all_pca \
    	(pc_0 float,\
    	pc_1 float,\
    	pc_2 float,\
    	pc_3 float,\
    	pc_4 float,\
    	pc_5 float,\
    	pc_6 float,\
    	pc_7 float,\
    	symbol TEXT, \
    	sector TEXT,\
    	label int,\
    	quarter int,\
    	PRIMARY KEY (symbol, quarter))')

    session.execute('CREATE TABLE IF not EXISTS top20companises_sectorwise \
    	(company_symbol TEXT, \
    	Name TEXT,\
    	Sector TEXT,\
    	PRIMARY KEY (company_symbol))')


    session.execute('CREATE TABLE IF not EXISTS news \
    	(id_column UUID,\
    	pub_date date,\
    	snippet TEXT,\
    	headline TEXT,\
    	news_desk TEXT,\
    	keywords TEXT,\
    	leading_paragraph TEXT,\
    	abstract TEXT,\
    	symbol TEXT,\
    	sector TEXT,\
    	PRIMARY KEY (id_column))')

    session.execute('CREATE TABLE IF not EXISTS common_news \
    	(pub_date date,\
    	symbol TEXT,\
    	combined_data TEXT,\
    	sector TEXT,\
    	PRIMARY KEY (pub_date, symbol))')

    session.execute('CREATE TABLE IF not EXISTS company_news \
    	(pub_date date,\
    	symbol TEXT,\
    	combined_data TEXT,\
    	sector TEXT,\
    	PRIMARY KEY (pub_date, symbol))')

    session.execute('CREATE TABLE IF not EXISTS combined_news \
        (pub_date date,\
        symbol TEXT,\
        sector_name TEXT,\
        combined_data TEXT,\
        PRIMARY KEY (pub_date, symbol))')

    session.execute('CREATE TABLE IF not EXISTS input_features \
    	(symbol TEXT,\
    	sector TEXT,\
    	RSI float,\
    	ROC float,\
    	ADOSC float,\
    	ATR float,\
    	EMA float,\
    	start_date date,\
    	end_date date,\
    	Sharpe_ratio float,\
    	average_returns float,\
    	wtd_sentiment float,\
    	PRIMARY KEY (symbol, start_date))')

    session.execute('CREATE TABLE IF not EXISTS common_sentiment \
    	(date date,\
    	symbol TEXT,\
    	sector_name TEXT,\
    	sentiment float,\
    	senti_label TEXT,\
    	PRIMARY KEY (date))')

    session.execute('CREATE TABLE IF not EXISTS company_sentiment \
    	(date date,\
    	symbol TEXT,\
    	sector_name TEXT,\
    	sentiment float,\
    	senti_label TEXT,\
    	PRIMARY KEY (date, symbol))')


##########################################################################################
#####################################WRITING TO DB########################################
##########################################################################################

def write_ohlc_quarter_1_to_cassandra(session, df, table_name="ohlc_quarter_1", KEYSPACE=KEYSPACE_NAME):

	query="INSERT INTO {}(symbol, sector, open, high, low, close) \
	VALUES (?,?,?,?,?,?);".format(table_name)
			
	prepared = session.prepare(query)
	pd_df = df.select("*").toPandas()
	pd_df = pd_df.reset_index()
	pd_df = pd_df[['symbol', 'sector', 'open', 'high', 'low', 'close']]

	for index, item in pd_df.iterrows():
		session.execute(prepared, (item['symbol'], item['sector'], float(item['open']), float(item['high']), float(item['low']), float(item['close'])))
		
def write_ohlc_to_cassandra(session, df, table_name="ohlc", KEYSPACE=KEYSPACE_NAME):

	query="INSERT INTO {}(symbol, sector, open, high, low, close, time) \
	VALUES (?,?,?,?,?,?,?);".format(table_name)
			
	prepared = session.prepare(query)
	pd_df = df.select("*").toPandas()
	pd_df = pd_df.reset_index()
	pd_df = pd_df[['symbol', 'sector', 'open', 'high', 'low', 'close', 'time']]

	for index, item in pd_df.iterrows():
		session.execute(prepared, (item['symbol'], item['sector'], float(item['open']), float(item['high']), float(item['low']), float(item['close']), pd.to_datetime(item['time'], format='%d/%m/%Y')))

def write_ohlcv_to_cassandra(session, df, table_name="ohlc", KEYSPACE=KEYSPACE_NAME):

	query="INSERT INTO {}(symbol, sector, open, high, low, close, volume, adjusted_close, dividend_amount, split_coefficient, time) \
	VALUES (?,?,?,?,?,?,?,?,?,?,?);".format(table_name)
			
	prepared = session.prepare(query)
	pd_df = df.select("*").toPandas()
	pd_df = pd_df.reset_index()
	pd_df = pd_df[['symbol', 'sector', 'open', 'high', 'low', 'close', 'volume', 'adjusted_close', 'dividend_amount', 'split_coefficient', 'time']]

	for index, item in pd_df.iterrows():
		session.execute(prepared, (item['symbol'], item['sector'], float(item['open']), float(item['high']), float(item['low']), float(item['close']), float(item['volume']), float(item["adjusted_close"]), float(item["dividend_amount"]), float(item["split_coefficient"]), pd.to_datetime(item['time'], format='%d/%m/%Y')))

def write_ohlcv_to_cassandra_from_pandas(session, df, table_name="ohlc", KEYSPACE=KEYSPACE_NAME):

    query="INSERT INTO {}(symbol, sector, open, high, low, close, volume, adjusted_close, dividend_amount, split_coefficient, time) \
    VALUES (?,?,?,?,?,?,?,?,?,?,?);".format(table_name)
            
    prepared = session.prepare(query)
    pd_df = df
    pd_df = pd_df.reset_index()
    pd_df = pd_df[['symbol', 'sector', 'open', 'high', 'low', 'close', 'volume', 'adjusted_close', 'dividend_amount', 'split_coefficient', 'time']]

    for index, item in pd_df.iterrows():
        session.execute(prepared, (item['symbol'], item['sector'], float(item['open']), float(item['high']), float(item['low']), float(item['close']), float(item['volume']), float(item["adjusted_close"]), float(item["dividend_amount"]), float(item["split_coefficient"]), pd.to_datetime(item['time'], format='%d/%m/%Y')))

def write_top20companises_sectorwise_to_cassandra(session, df, table_name="top20companises_sectorwise", KEYSPACE=KEYSPACE_NAME):
	query="INSERT INTO {}(company_symbol, Name, Sector) \
	VALUES (?,?,?);".format(table_name)
			
	prepared = session.prepare(query)
	pd_df = df.select("*").toPandas()
	pd_df = pd_df.reset_index()
	pd_df = pd_df[['company_symbol', 'Name', 'Sector']]

	for index, item in pd_df.iterrows():
		session.execute(prepared, (item['company_symbol'], item['Name'], item['Sector'] ))	

def write_pca_quarter_1_to_cassandra(session, df, table_name="pca_quarter_1", KEYSPACE=KEYSPACE_NAME):

	query="INSERT INTO {}(pc_0, pc_1, pc_2, pc_3, pc_4, pc_5, pc_6, pc_7, symbol, sector, label) \
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);".format(table_name)
			
	prepared = session.prepare(query)
	pd_df = df.select("*").toPandas()
	pd_df = pd_df.reset_index()
	pd_df = pd_df[["PC_0", "PC_1", "PC_2", "PC_3", "PC_4", "PC_5", "PC_6", "PC_7", "symbol", "sector", "label"]]

	for index, item in pd_df.iterrows():
		session.execute(prepared, (float(item['PC_0']), float(item['PC_1']), \
			float(item['PC_2']), float(item['PC_3']), float(item['PC_4']), float(item['PC_5']), \
			float(item['PC_6']), float(item['PC_7']), item['symbol'], item['sector'], int(item['label']) ))

def write_news_to_cassandra(session, df, table_name="news", KEYSPACE=KEYSPACE_NAME):

	query="INSERT INTO {}(id_column, pub_date, snippet, headline, news_desk, keywords, leading_paragraph, abstract, symbol, sector) \
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);".format(table_name)
			
	prepared = session.prepare(query)
	pd_df = df
	pd_df = pd_df.reset_index()
	pd_df = pd_df[['date', 'snippet', 'headline', 'news_desk','keywords', 'leading_paragraph', 'abstract', 'symbol', 'sector']]

	for index, item in pd_df.iterrows():
		session.execute(prepared, (uuid.uuid1(), item["date"], item["snippet"], item["headline"], item["news_desk"],\
		" ".join(item["keywords"]), item["leading_paragraph"], item["abstract"], item["symbol"], item["sector"] ))

def write_all_pca_to_cassandra(session, df, table_name="all_pca", KEYSPACE=KEYSPACE_NAME):

	query="INSERT INTO {}(pc_0, pc_1, pc_2, pc_3, pc_4, pc_5, pc_6, pc_7, symbol, sector, label, quarter) \
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);".format(table_name)
			
	prepared = session.prepare(query)
	pd_df = df.select("*").toPandas()
	pd_df = pd_df.reset_index()
	pd_df = pd_df[["PC_0", "PC_1", "PC_2", "PC_3", "PC_4", "PC_5", "PC_6", "PC_7", "symbol", "sector", "label", "Quarter"]]

	for index, item in pd_df.iterrows():
		session.execute(prepared, (float(item['PC_0']), float(item['PC_1']), \
			float(item['PC_2']), float(item['PC_3']), float(item['PC_4']), float(item['PC_5']), \
			float(item['PC_6']), float(item['PC_7']), item['symbol'], item['sector'], int(item['label']), int(item['Quarter']) ))

def write_common_news_to_cassandra(session, df, table_name="common_news", KEYSPACE=KEYSPACE_NAME):

	query="INSERT INTO {}(pub_date, combined_data, symbol, sector) \
	VALUES (?, ?, ?, ?);".format(table_name)
			
	prepared = session.prepare(query)
	pd_df = df.select("*").toPandas()
	pd_df = pd_df.reset_index()
	pd_df = pd_df[['date', 'combined_data', 'symbol', 'sector_name']]

	for index, item in pd_df.iterrows():
		session.execute(prepared, (item["date"], item["combined_data"], item["symbol"], item["sector_name"] ))

def write_common_news_to_cassandra_from_pandas(session, df, table_name="common_news", KEYSPACE=KEYSPACE_NAME):

    query="INSERT INTO {}(pub_date, combined_data, symbol, sector) \
    VALUES (?, ?, ?, ?);".format(table_name)
            
    prepared = session.prepare(query)
    pd_df = df
    pd_df = pd_df.reset_index()
    pd_df = pd_df[['date', 'combined_data', 'symbol', 'sector_name']]

    for index, item in pd_df.iterrows():
        session.execute(prepared, (item["date"], item["combined_data"], item["symbol"], item["sector_name"] ))

def write_company_news_to_cassandra(session, df, table_name="company_news", KEYSPACE=KEYSPACE_NAME):

	query="INSERT INTO {}(pub_date, combined_data, symbol, sector) \
	VALUES (?, ?, ?, ?);".format(table_name)
			
	prepared = session.prepare(query)
	pd_df = df.select("*").toPandas()
	pd_df = pd_df.reset_index()
	pd_df = pd_df[['date', 'combined_data', 'symbol', 'sector_name']]

	for index, item in pd_df.iterrows():
		session.execute(prepared, (item["date"], item["combined_data"], item["symbol"], item["sector_name"] ))

def write_company_news_to_cassandra_from_pandas(session, df, table_name="company_news", KEYSPACE=KEYSPACE_NAME):

    query="INSERT INTO {}(pub_date, combined_data, symbol, sector) \
    VALUES (?, ?, ?, ?);".format(table_name)
            
    prepared = session.prepare(query)
    pd_df = df
    pd_df = pd_df.reset_index()
    pd_df = pd_df[['date', 'combined_data', 'symbol', 'sector_name']]

    for index, item in pd_df.iterrows():
        session.execute(prepared, (item["date"], item["combined_data"], item["symbol"], item["sector_name"] ))

def write_combined_news_to_cassandra_from_pandas(session, df, table_name="combined_news", KEYSPACE=KEYSPACE_NAME):

    query="INSERT INTO {}(pub_date, symbol, sector_name, combined_data) \
    VALUES (?, ?, ?, ?);".format(table_name)
            
    prepared = session.prepare(query)
    pd_df = df
    pd_df = pd_df.reset_index()
    pd_df = pd_df[['date', "symbol", "sector_name", "combined_data"]]

    for index, item in pd_df.iterrows():
        session.execute(prepared, (item["date"], item["symbol"], item["sector_name"], item["combined_data"] ))

def write_common_sentiment_to_cassandra(session, df, table_name="common_sentiment", KEYSPACE=KEYSPACE_NAME):

	query="INSERT INTO {}(date, symbol, sector_name, sentiment, senti_label) \
	VALUES (?, ?, ?, ?, ?);".format(table_name)
			
	prepared = session.prepare(query)
	pd_df = df.select("*").toPandas()
	pd_df = pd_df.reset_index()
	pd_df = pd_df[['date', 'symbol', 'sector_name', 'sentiment', 'senti_label']]

	for index, item in pd_df.iterrows():
		session.execute(prepared, (item["date"], item["symbol"], item["sector_name"], float(item["sentiment"]), item["senti_label"]))

def write_company_sentiment_to_cassandra(session, df, table_name="company_sentiment", KEYSPACE=KEYSPACE_NAME):

	query="INSERT INTO {}(date, symbol, sector_name, sentiment, senti_label) \
	VALUES (?, ?, ?, ?, ?);".format(table_name)
			
	prepared = session.prepare(query)
	pd_df = df.select("*").toPandas()
	pd_df = pd_df.reset_index()
	pd_df = pd_df[['date', 'symbol', 'sector_name', 'sentiment', 'senti_label']]

	for index, item in pd_df.iterrows():
		session.execute(prepared, (item["date"], item["symbol"], item["sector_name"], float(item["sentiment"]), item["senti_label"]))

def write_input_features_to_cassandra(session, df, table_name="input_features", KEYSPACE=KEYSPACE_NAME):

	query="INSERT INTO {}(symbol, sector, RSI, ROC, ADOSC, ATR, EMA, start_date, end_date, Sharpe_ratio, \
	average_returns, wtd_sentiment) \
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);".format(table_name)
			
	prepared = session.prepare(query)
	pd_df = df
	pd_df = pd_df.reset_index()
	pd_df = pd_df[["symbol", "sector", "RSI", "ROC", "ADOSC", "ATR", "EMA", "start_date", "end_date", "Sharpe_ratio", \
	"average_returns", "wtd_sentiment"]]

	for index, item in pd_df.iterrows():
		session.execute(prepared, (item["symbol"], item["sector"], float(item["RSI"]), float(item["ROC"]),\
		float(item["ADOSC"]), float(item["ATR"]), float(item["EMA"]), item["start_date"], item["end_date"],\
		float(item["Sharpe_ratio"]), float(item["average_returns"]), float(item["wtd_sentiment"])))

##############################################################################################
#####################################GENERATING TABLES########################################
##############################################################################################


def generate_ohlc_quarter_1_tables():
	# Create table
	session = connect_to_cassandra()
	create_cassandra_tables(session)

	quarter1_data = get_quarter_1_ohlc_data_from_csv("ohlcvquarter_1.csv")
	write_ohlc_quarter_1_to_cassandra(session, quarter1_data, "ohlc_quarter_1", KEYSPACE_NAME)
	print("Data written to ohlc_quarter_1 table")

def generate_ohlc_tables():
	# Create table
	session = connect_to_cassandra()
	create_cassandra_tables(session)

	ohlc_data = get_all_ohlc_data_from_csv("ohlcv.csv")
	write_ohlc_to_cassandra(session, ohlc_data, "ohlc", KEYSPACE_NAME)
	print("Data written to ohlc table")

def generate_ohlcv_tables():
	# Create table
	session = connect_to_cassandra()
	create_cassandra_tables(session)

	ohlcv_data = get_all_ohlcv_data_from_csv("ohlcv.csv")
	write_ohlcv_to_cassandra(session, ohlcv_data, "ohlcv", KEYSPACE_NAME)
	print("Data written to ohlcv table")

def generate_ohlcv_tables_from_api():
    # Create table
    session = connect_to_cassandra()
    create_cassandra_tables(session)

    ohlcv_data = cct.generate_ohlcv_data()
    write_ohlcv_to_cassandra_from_pandas(session, ohlcv_data, "ohlcv", KEYSPACE_NAME)
    print("Data written to ohlcv table")

def generate_common_news_tables_from_api():
#     # Create table
    session = connect_to_cassandra()
    create_cassandra_tables(session)

    common_news_data = fnd.read_files("../../Data-Acquisition/news-data/Common_news/")
    write_common_news_to_cassandra_from_pandas(session, common_news_data, "common_news", KEYSPACE_NAME)
    print("Data written to common_news table using api calls")

def generate_company_news_tables_from_api():
#     # Create table
    session = connect_to_cassandra()
    create_cassandra_tables(session)

    common_news_data = fnd.read_files("../../Data-Acquisition/news-data/Company_news/")
    write_company_news_to_cassandra_from_pandas(session, common_news_data, "company_news", KEYSPACE_NAME)
    print("Data written to company_news table using api calls")

def generate_top20companises_sectorwise_tables():
	# Create table
	session = connect_to_cassandra()
	create_cassandra_tables(session)

	top20companises_sectorwise_data = get_top20companises_sectorwise_data_from_csv("top20companises_sectorwise.csv")
	write_top20companises_sectorwise_to_cassandra(session, top20companises_sectorwise_data, "top20companises_sectorwise", KEYSPACE_NAME)
	print("Data written to top20companises_sectorwise table")

def generate_pca_quarter_1_tables():
	# Create table
	session = connect_to_cassandra()
	create_cassandra_tables(session)

	pca_quarter_1_data = get_pca_quarter_1_data_from_csv("1_pca_qt_result.csv")
	write_pca_quarter_1_to_cassandra(session, pca_quarter_1_data, "pca_quarter_1", KEYSPACE_NAME)
	print("Data written to pca_quarter_1 table")


def generate_all_pca_tables():
	# Create table
	session = connect_to_cassandra()
	create_cassandra_tables(session)

	all_pca_data = get_all_pca_data_from_csv("./pca_folder/pca_folder_cl4/pca_complete_result.csv")
	write_all_pca_to_cassandra(session, all_pca_data, "all_pca", KEYSPACE_NAME)
	print("Data written to all_pca table")

def generate_common_news_tables():
	# Create table
	session = connect_to_cassandra()
	create_cassandra_tables(session)

	common_news_data = get_common_news_data_from_csv("news_common_till_april.csv")
	write_common_news_to_cassandra(session, common_news_data, "common_news", KEYSPACE_NAME)
	print("Data written to common_news table")

def generate_company_news_tables():
	# Create table
	session = connect_to_cassandra()
	create_cassandra_tables(session)

	company_news_data = get_company_news_data_from_csv("news_company_till_april.csv")
	write_company_news_to_cassandra(session, company_news_data, "company_news", KEYSPACE_NAME)
	print("Data written to company_news table")

# After generating news table from Kafka, call this method to get the final combined table
def generate_combined_news_tables():
    # Create table
    session = connect_to_cassandra()
    create_cassandra_tables(session)

    company_news_data = get_combine_news_text_content_one_column()
    write_combined_news_to_cassandra_from_pandas(session, df, table_name="combined_news", KEYSPACE=KEYSPACE_NAME)
    print("Data written to combined_news table")

##############################################################################################
########################################RESULTS FOR UI########################################
##############################################################################################

def load_and_get_table_df(table_name, KEYSPACE=KEYSPACE_NAME):
    sqlContext = SQLContext(sc)
    table_df = sqlContext.read\
        .format("org.apache.spark.sql.cassandra")\
        .options(table=table_name, keyspace=KEYSPACE)\
        .load()
    return table_df

def get_company_lst(sector_name):
	top20companises_sectorwise = load_and_get_table_df("top20companises_sectorwise", KEYSPACE_NAME)
	company_list = top20companises_sectorwise.where(top20companises_sectorwise["Sector"] == sector_name).select("company_symbol").collect()
	name_list = top20companises_sectorwise.where(top20companises_sectorwise["Sector"] == sector_name).select("Name").collect()
	
	return [company_list[i].__getitem__("company_symbol") + " - " +  name_list[i].__getitem__("Name") for i in range(len(company_list))]

def get_feature_df(df, company_symbol=None, feature="Trend"):
	if(company_symbol != None):
		df_comp = df[df["symbol"] == company_symbol]
	else:
		df_comp = df
		
	df_comp = df_comp.sort_values(by=['time'])
	if(feature == "Trend"):
		df_comp[feature] = ta.EMA(df_comp.close, timeperiod = 90)
	elif(feature == "Volatility"):
		df_comp[feature] = ta.ATR(df_comp.high, df_comp.low, df_comp.close, timeperiod=90)
	elif(feature == "Volume"):
		df_comp[feature] = ta.ADOSC(df_comp.high, df_comp.low, df_comp.close, df_comp.volume, fastperiod=30, slowperiod=90)
	elif(feature == "Momemtum"):
		df_comp[feature+"_1"] = ta.RSI(df_comp.close, timeperiod = 90)
		df_comp[feature+"_2"] = ta.ROC(df_comp.close, timeperiod = 90)
	elif(feature == "Open_Close"):
		df_comp[feature+"_1"] = df_comp["open"]
		df_comp[feature+"_2"] = df_comp["close"]

	return df_comp

##############################################################################################
	
def generate_all_necessary_tables_at_once():
	# Generate OHLCV data
    print("generate_ohlcv_tables")
    generate_ohlcv_tables_from_api()
	
	# Generate Top20 Companies sectorwise table
    print("generate_top20companises_sectorwise_tables")
    generate_top20companises_sectorwise_tables()

	# Generate all pca table
    print("generate_all_pca_tables")
    generate_all_pca_tables()

	# Generate common news table
    print("generate_common_news_tables")
	# generate_common_news_tables()
    generate_common_news_tables_from_api()

	# Generate company news table
    print("generate_company_news_tables")
	# generate_company_news_tables()
    generate_company_news_tables_from_api()

################################################################################################

# Uncomment and Run the below command to generate only the necessary tables in one go
# generate_all_necessary_tables_at_once()






