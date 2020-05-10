
import sentiment_generation
import ohlc_features
import pandas as pd
import numpy as np
import data_operations

"""
# Command to run the file (makes connection to cassandra database)

spark-submit --packages datastax:spark-cassandra-connector:2.4.0-s_2.11 input_features.py

"""

# cluster_seeds = ['127.0.0.1']
# spark = SparkSession.builder.appName('Stock Analysis').config('spark.cassandra.connection.host',\
#                                                                       ','.join(cluster_seeds)).getOrCreate()
# sc = spark.sparkContext

# Our cassandra keyspace
KEYSPACE_NAME = 'stocks'

#generate common news sentiment
def generate_common_sentiment_features_table():
    common_news_data = data_operations.get_common_news_from_db(tablename="common_news", KEYSPACE=KEYSPACE_NAME)
    common_senti_df=sentiment_generation.create_sentiment(common_news_data)

    write_common_sentiment_to_cassandra(session, common_senti_df, "common_sentiment", KEYSPACE_NAME)
    print("Data written to common_sentiment table")

#generate company wise news sentiment

def generate_company_sentiment_features_table():    

    company_news_data = data_operations.get_company_news_from_db(tablename="company_news", KEYSPACE=KEYSPACE_NAME)
    company_senti_df=sentiment_generation.create_sentiment(company_news_data)
    
    write_company_sentiment_to_cassandra(session, company_senti_df, "company_sentiment", KEYSPACE_NAME)
    print("Data written to company_sentiment table")


#Integrate stock features and sentiment features to form input features for analysis
def generate_input_features_table():

    final_input_features_df=pd.DataFrame()
        
    session = data_operations.connect_to_cassandra()
    data_operations.create_cassandra_tables(session)

    #Read all stock features and sentiment features from database

    ohlc_data=data_operations.get_all_ohlcv_data_from_db(tablename="ohlcv", KEYSPACE=KEYSPACE_NAME)
    ohlc_feature_df=ohlc_features.create_ohlc_features(ohlc_data)

    sharpe_returns_df=ohlc_features.create_sharpe_returns(ohlc_data)  
    sharpe_returns_df=sharpe_returns_df.drop(columns=['end_date']) 


    stock_features=pd.merge(ohlc_feature_df,sharpe_returns_df,on=['symbol','start_date'],how='left')
    stock_features.dropna(inplace=True)

    company_sentiment=data_operations.get_company_sentiment_data_from_db(tablename="company_sentiment", KEYSPACE=KEYSPACE_NAME)
    common_sentiment=data_operations.get_common_sentiment_data_from_db(tablename="common_sentiment", KEYSPACE=KEYSPACE_NAME)


    quarter_list=["2018-04-01", "2018-07-01", "2018-10-01", "2019-01-01", "2019-04-01", "2019-07-01", "2019-10-01", "2020-01-01", "2020-04-01"] 

     #integrate stock features with sentiment 
    for idx in range(1, len(quarter_list)):

        
        stock_quarter = stock_features[(stock_features["start_date"] >= quarter_list[idx-1]) & (stock_features["start_date"] <quarter_list[idx])]
        common_quarter = common_sentiment[(common_sentiment["date"] >= quarter_list[idx-1]) & (common_sentiment["date"]< quarter_list[idx])]


        company_quarter = company_sentiment[(company_sentiment["date"] >= quarter_list[idx-1]) & (company_sentiment["date"]< quarter_list[idx])]
        company_mean=company_quarter[['symbol','sentiment']].groupby('symbol').mean().reset_index()



        input_feature_df=pd.merge(stock_quarter,company_mean,on='symbol',how='left')       


        input_feature_df['sentiment']=input_feature_df['sentiment'].fillna(0)
        input_feature_df['common_senti']=common_quarter['sentiment'].mean()


         #create weighted sentiment feature from common news and company news
        input_feature_df['wtd_sentiment']=0.25*input_feature_df['common_senti']+0.75*input_feature_df['sentiment']
        input_feature_df.drop(columns=['sentiment','common_senti'],inplace=True)
        
        final_input_features_df=pd.concat([final_input_features_df,input_feature_df],ignore_index=True,axis=0)
        
    # Perform suitable operations to create the feature

    data_operations.write_input_features_to_cassandra(session,final_input_features_df, "input_features", KEYSPACE_NAME)
    print("Data written to input_features table")

print("generate_input_features_table")
generate_input_features_table()


