import pandas as pd 
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from matplotlib import rcParams

rcParams['axes.titlepad'] = 20 
import os.path
import sys


print(sys.path.append(os.path.abspath(os.path.join('../Code/', 'data_operations'))))
import data_operations

KEYSPACE_NAME = 'stocks'

#create new folder to save results of EDA
current_directory = os.getcwd()
final_directory = os.path.join(current_directory, 'news_EDA_plots')
final_directory
if not os.path.exists(final_directory):
    os.makedirs(final_directory)


#read input data    

company_stock=data_operations.get_company_sentiment_data_from_db(tablename="company_sentiment", KEYSPACE=KEYSPACE_NAME)
common_stock=data_operations.get_common_sentiment_data_from_db(tablename="common_sentiment", KEYSPACE=KEYSPACE_NAME)

# company_stock=pd.read_csv("company_sentiment.csv")

# company_stock['date']=company_stock['date'].apply(lambda x: pd.to_datetime(x).tz_convert('US/Eastern'))
# company_stock['date']=company_stock['date'].dt.tz_localize(None)
# company_stock['date']=company_stock['date'].dt.date


# company_stock.shape

# common_stock=pd.read_csv("common_sentiment.csv")
# common_stock.shape

# common_stock['date']=common_stock['date'].apply(lambda x: pd.to_datetime(x).tz_convert('US/Eastern'))
# common_stock['date']=common_stock['date'].dt.tz_localize(None)
# common_stock['date']=common_stock['date'].dt.date

#### Merge common news and comapny news and created weighted sentiment

df=pd.merge(common_stock, company_stock, on='date', how='outer',suffixes=('_common', '_company'))
df = df[df['symbol_company'].notna()].reset_index(drop=True)


print("\nno. of columns in dataframe=", df.columns)

print("\nshape of the dataframe=",df.shape)

df['wt_senti']=0.25*df['sentiment_common']+0.75*df['sentiment_company']

quarter_list=["2018-04-01", "2018-07-01", "2018-10-01", "2019-01-01", "2019-04-01", "2019-07-01", "2019-10-01", "2020-01-01", "2020-04-01"]




from functools import partial

f = partial(pd.to_datetime)

#### Top 10 companies with most postive news quarterwise

for idx in range(1,len(quarter_list)):
    df_quarter = df[(df["date"] >= f(quarter_list[idx-1]))  & (df["date"] < f(quarter_list[idx]))]
    df_quarter_open = df_quarter[["symbol_company", "wt_senti"]]
    
    company_groups = df_quarter_open.groupby("symbol_company").sum().reset_index()
    
    sorted_company_groups = company_groups.sort_values("wt_senti",  ascending=False)
    
    top_companies = sorted_company_groups[1:11]
    quarter_str = "Top 10 Companies with most positive news in quarter starting from "+quarter_list[idx - 1]

    top_companies.plot.bar(x="symbol_company", y="wt_senti", rot=90, title=quarter_str)
    plt.title(quarter_str)
    plt.ylabel("cumulative sum of sentiment values" )
    plt.savefig(final_directory+"/"+"top10_comp_sentiment_qtr"+str(idx),bbox_inches='tight')

sector_list=list(df['sector_name_company'].unique())


#### Sectorwise  plot of sentiment for over 2 years perios of time strating from April 2018

rcParams['figure.figsize'] = 10, 5
for sector in sector_list:   
    df_sector = df[df["sector_name_company"] == sector]
    df_group_date = df_sector[['date','wt_senti']].groupby("date").mean().reset_index()
    
    
    plt.plot(df_group_date['date'].tolist(), df_group_date['wt_senti'].tolist())    
    
    plt.title("Sentiment for "+  sector  + "sector for over two year period of time")    
    plt.xlabel("Time period")
    plt.ylabel("Sentiment")
    plt.savefig(final_directory+"/"+sector+"_sentiment_for_2yr")
    
  
    

