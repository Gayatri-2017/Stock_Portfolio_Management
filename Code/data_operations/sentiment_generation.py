"""
Created on Sat Mar  7 18:52:57 2020

@author: arnav
"""
# This code is responsible for genrating the sentiment score for the stocks as well as news data.

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('sentiment code').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.ml.feature import RegexTokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml import Pipeline, PipelineModel
import nltk

nltk.download('words')

from nltk.sentiment.vader import SentimentIntensityAnalyzer  
from nltk.stem import WordNetLemmatizer

import string
import re
import pandas as pd


def strip_non_ascii(data_str):
      
    stripped = (c for c in data_str if 0 < ord(c) < 127)
    return ''.join(stripped)
strip_non_ascii_udf = udf(strip_non_ascii, StringType())


def strip_nonenglish(data_str):
    words = set(nltk.corpus.words.words())
    data_str=" ".join(w for w in nltk.wordpunct_tokenize(data_str) \
         if w.lower() in words or not w.isalpha())
    return(data_str)
strip_nonenglish_udf = udf(strip_nonenglish, StringType())    


def remove_features(data_str):
    # compile regex
    url_re = re.compile('https?://(www.)?\w+\.\w+(/\w+)*/?')
    punc_re = re.compile('[%s]' % re.escape(string.punctuation))
    num_re = re.compile('(\\d+)')

    alpha_num_re = re.compile("^[a-z0-9_.]+$")

    # remove hyperlinks
    data_str = url_re.sub(' ', data_str)
    
    # remove puncuation
    data_str = punc_re.sub(' ', data_str)
    # remove numeric 'words
    data_str = num_re.sub(' ', data_str)
    
    return(data_str)
remove_features_udf = udf(remove_features, StringType())


#join tokens
def join_text(x):
    joinedTokens_list = []
    x = " ".join(x)
    return x
join_udf = udf(lambda x: join_text(x), StringType())


def lemma(data_str):   
   
    lemmatizer = WordNetLemmatizer()
    text = data_str[2]
    lemm_word= lemmatizer.lemmatize(text)
    return(data_str[0],data_str[1],lemm_word,data_str[3])	



def sentiment_analyzer_scores(data_str):
    analyzer = SentimentIntensityAnalyzer() 

    text = data_str[2]
    score = analyzer.polarity_scores(text)['compound']
    return(data_str[0], data_str[1],data_str[3],score)


    
def senti(col):
    if col>0:
        return 'positive'
    elif col<0:
        return 'negative'
    else:
        return 'neutral'
senti_udf = udf(senti, StringType())     




def create_sentiment( news_df):
    
   
    news_df['date']=pd.to_datetime(news_df['date'])
    news_df=news_df[['date','symbol','combined_data', "sector_name"]]
    news_df=spark.createDataFrame(news_df)
     
    news_text_lower = news_df.select(news_df['date'],news_df['symbol'], functions.lower(news_df['combined_data']).alias("content"), news_df["sector_name"]) 
       
   

    #text preprocessing
    df = news_text_lower.withColumn('text_non_asci',strip_non_ascii_udf(news_text_lower['content']))
    rm_df =df.withColumn('clean_text',remove_features_udf(df['text_non_asci']))
   



    regexTokenizer = RegexTokenizer(gaps = False, pattern = '\w+', inputCol = 'clean_text', outputCol = 'token')
    stopWordsRemover = StopWordsRemover(inputCol = 'token', outputCol = 'no_stopword')

    my_pipeline = Pipeline(stages=[regexTokenizer, stopWordsRemover])
    reg_model=my_pipeline.fit(rm_df)
    reg_df=reg_model.transform(rm_df)


    reg_joined=reg_df.withColumn("fine_text", join_udf( reg_df['no_stopword']))
    reg_joined1=reg_joined.withColumn('eng_text',strip_nonenglish_udf(reg_joined['fine_text']))
    clean_df=reg_joined1.select('date','symbol','eng_text','sector_name')
    #clean_df.show(2)
        


    #calculate sentiment score
    
    news_rdd=clean_df.rdd
    lemma_rdd=news_rdd.map(lemma)	
    score_rdd = lemma_rdd.map(sentiment_analyzer_scores)
    news_sentiment = spark.createDataFrame(score_rdd).cache()


    news_sentiment= news_sentiment.withColumnRenamed('_1', 'date')
    news_sentiment= news_sentiment.withColumnRenamed('_2', 'symbol')
    news_sentiment= news_sentiment.withColumnRenamed('_3', 'sector_name')
    news_sentiment= news_sentiment.withColumnRenamed('_4', 'sentiment')
    news_sentiment=news_sentiment.withColumn('senti_label',senti_udf(news_sentiment['sentiment']))
    stock_news=news_sentiment.toPandas()
    
    stock_news['date']=stock_news['date'].apply(lambda x: pd.to_datetime(x).tz_convert('US/Eastern'))
    stock_news['date']=stock_news['date'].dt.tz_localize(None)
    stock_news['date']=stock_news['date'].dt.date
    stock_news['date']=pd.to_datetime(stock_news['date'])
    return(stock_news)
   # print(stock['senti_label'].value_counts())	

	

   

    


   
