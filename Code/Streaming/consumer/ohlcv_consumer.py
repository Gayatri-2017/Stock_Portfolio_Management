
import ast
import time
from kafka import KafkaConsumer
from cassandra.cluster import Cluster,NoHostAvailable

class CassandraStorage(object):
    
    def __init__(self):
        
        self.key_space="stocks"
        
        # init a Cassandra cluster instance
        cluster = Cluster()
        
        # start Cassandra server before connecting       
        try:
            self.session = cluster.connect()
        except NoHostAvailable:
            print("Fatal Error: need to connect Cassandra server")            
        else:
            self.create_table()
        
    
    def string_to_float(self, string):
        if type(string)==str:
            return float(string)
        else:
            return string

    def create_table(self):
        """
        create Cassandra table of stock if not exist
        :return: None
        
        """
        self.session.execute("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'} AND durable_writes = 'true'" % "stocks")
        self.session.set_keyspace(self.key_space) 
        
        # create table for historical data
        self.session.execute("CREATE TABLE IF NOT EXISTS {} ( \
                                    	TIME timestamp,           \
                                    	SYMBOL text,              \
                                    	OPEN float,               \
                                    	HIGH float,               \
                                    	LOW float,                \
                                    	CLOSE float,              \
                                    ADJUSTED_CLOSE float,     \
                                    VOLUME float,             \
                                    dividend_amount float,    \
                                    split_coefficient float,  \
                                    PRIMARY KEY (SYMBOL,TIME));".format('ohlcv_data')) 

    def kafka_consumer(self):
        
        self.consumer1 = KafkaConsumer(
                                    "stock_streaming1",
                                    bootstrap_servers='127.0.0.1:9092')    
        
    
    def stream_to_cassandra(self):
        
        for msg in self.consumer1:
            # decode msg value from byte to utf-8
            data=ast.literal_eval(msg.value.decode("utf-8"))
            
            for dict_data in data:
                for key in ['open', 'high', 'low', 'close', 'volume','adjusted_close','dividend_amount','split_coefficient']:
                    dict_data[key]=self.string_to_float(dict_data[key])
                
                if(dict_data["time"] > "2018-04-01"):
                    query="INSERT INTO {}(time, symbol,open,high,low,close,adjusted_close,volume,dividend_amount,split_coefficient) VALUES ('{}','{}',{},{},{},{},{},{},{},{});".format("ohlcv_data", dict_data['time'],
                                dict_data['symbol'],dict_data['open'], \
                                dict_data['high'],dict_data['low'],dict_data['close'],dict_data['adjusted_close'],dict_data['volume'],dict_data['dividend_amount'],dict_data['split_coefficient']) 

                    self.session.execute(query)
                    print("Stored {}\'s OHLCV data at {}".format(dict_data['symbol'],dict_data['time']))
   

    def delete_table(self,table_name):
        self.session.execute("DROP TABLE {}".format(table_name))
        
def main_realtime():
    """
    main funtion to store realtime data; recommend to set tick=False, as getting tick data would cause rate limiting error from API 
    """
    database=CassandraStorage()
    database.kafka_consumer()
    
    database.stream_to_cassandra()
            
if __name__=="__main__":
    
    main_realtime()
    
    
