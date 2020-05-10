from kafka import KafkaProducer
import pandas as pd
import common_news_producer as comm_prod
import company_news_producer as comp_prod
import ohlcv_producer as op
import schedule

if __name__=="__main__":

    # init an instance of KafkaProducer
    producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')

    schedule.every(1).seconds.do(op.ohlcv_kafka_producer,producer)
    schedule.every(1).seconds.do(comm_prod.kafka_producer_common_news,producer)
    schedule.every(3).seconds.do(comp_prod.kafka_producer_company_news, producer, row_id=0)


    while True:
        schedule.run_pending()