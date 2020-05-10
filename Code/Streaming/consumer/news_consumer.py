import ast
import time
from kafka import KafkaConsumer
from cassandra.cluster import Cluster,NoHostAvailable
import pandas as pd
import json
import pyjq

import sys, os
sys.path.append(os.path.abspath(os.path.join('../..', 'data_operations')))

import data_operations

# Command to run the file (makes connection to cassandra database)
# For consuming company_news
# python news_consumer.py company_news
# For consuming common_news
# python news_consumer.py common_news

# To run the consumer independently, run the following command on the console
# kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic <topic_name≥ -- from-beginning

KEYSPACE_NAME = 'stocks'

def kafka_consumer(topic_name):

    consumer = KafkaConsumer(topic_name, bootstrap_servers='127.0.0.1:9092')

    return consumer

def parse_news(data):

    print("Inside parse_news, data = \n", data)
    json_data = json.loads(data)
    print("Inside parse_news, json_data = \n", json_data)

    jq_query = f'{{symbol: .symbol, sector: .sector, snippet: .snippet, headline: .headline .main, \
    date: .pub_date, news_desk: .news_desk, keywords: .keywords | [.[] | .value],\
    leading_paragraph: .lead_paragraph, abstract: .abstract}} '

    output = pyjq.all(jq_query, json_data)
    news_df = pd.DataFrame(output) 

    if(not news_df.empty):

        print("news_df.columns=\n", news_df.columns)

        news_df["date"] = pd.to_datetime(news_df["date"]).dt.date
        
        news_df = news_df.fillna(" ")
        
        news_df = news_df.groupby(['date', "symbol", "sector"]).agg({'snippet': ' '.join, 'headline': ' '.join, \
            'news_desk': ' '.join, "keywords": "sum", "leading_paragraph": ' '.join, \
            "abstract": ' '.join}).reset_index()

        print("news_df = \n", news_df)
        print("news_df.columns = \n", news_df.columns)

        return news_df

    
def news_to_cassandra(session, consumer):

    print("Inside news_to_cassandra")
    for msg in consumer:
        dict_data=ast.literal_eval(msg.value.decode("utf-8"))
        print("*"*80)

        news_df = parse_news(json.dumps(dict_data))
        data_operations.write_news_to_cassandra(session, news_df, table_name="news", KEYSPACE=KEYSPACE_NAME)

        
def generate_news_data(topic_name):
    session = data_operations.connect_to_cassandra()
    consumer = kafka_consumer(topic_name)
    data_operations.create_cassandra_tables(session)
    
    news_to_cassandra(session, consumer)
    
if __name__=="__main__":
    topic_name = str(sys.argv[1])
    generate_news_data(topic_name)
    