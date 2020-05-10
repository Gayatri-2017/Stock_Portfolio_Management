#this program is used to process only top 20 companies to be used in our analysis from constituents_csv.csv file.
import pandas as pd

def create_sector_csv():
    # dataf = pd.read_csv("constituents_csv.csv")
    # path while calling this function from data_operations
    dataf = pd.read_csv("../../Data-Acquisition/ohlcv-data/constituents_csv.csv")
    Industry_list = list(dataf["Sector"].unique())
    # Creating an empty Dataframe with column names only
    dfObj = pd.DataFrame()
    for industry_name in Industry_list:
        df = dataf[dataf["Sector"] == industry_name]
        if df.count()[0] > 20:
            df1 = df.iloc[:20]
            dfObj = dfObj.append(df1, ignore_index=True)

    dfObj.to_csv("top20companises_sectorwise.csv")
    return
