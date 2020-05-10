import pandas as pd
import talib as ta
import numpy as np

# No warnings about setting value on copy of slice
pd.options.mode.chained_assignment = None

def create_ohlc_features(ohlcv_df):
    quarter_list=["2018-04-01", "2018-07-01", "2018-10-01", "2019-01-01", "2019-04-01", "2019-07-01", "2019-10-01", "2020-01-01", "2020-04-01"]
    df_copy = ohlcv_df.copy()
    df_copy["time"] = pd.to_datetime(df_copy.time)
    features_df = pd.DataFrame([])
    for idx in range(1, len(quarter_list)):
        df_quarter_data = df_copy[(df_copy["time"] >= quarter_list[idx-1]) & (df_copy["time"]< quarter_list[idx])]
        symbol_list = df_quarter_data["symbol"].unique()
        comp_list = []
        for company in symbol_list:
            df_cmp_qtr_data = df_quarter_data[df_quarter_data["symbol"] == company]
            count = len(df_cmp_qtr_data)
            if(count > 1):
                #Momemtum indicator - 1
                df_cmp_qtr_data["RSI"] = ta.RSI(df_cmp_qtr_data.close, timeperiod = count-1).fillna(0)
                #Momemtum indicator - 2
                df_cmp_qtr_data["ROC"] = ta.ROC(df_cmp_qtr_data.close, timeperiod = count-1).fillna(0)
                count_fast = count/2
                #volume indicator
                df_cmp_qtr_data["ADOSC"] = ta.ADOSC(df_cmp_qtr_data.high, df_cmp_qtr_data.low, df_cmp_qtr_data.close, df_cmp_qtr_data.volume, fastperiod=count_fast, slowperiod=count).fillna(0)
                #volatility indicator
                df_cmp_qtr_data["ATR"] = ta.ATR(df_cmp_qtr_data.high, df_cmp_qtr_data.low, df_cmp_qtr_data.close, timeperiod=count-1).fillna(0)
                #trend indicator
                df_cmp_qtr_data["EMA"] = ta.EMA(df_cmp_qtr_data.close, timeperiod = count).fillna(0)
            else: 
                df_cmp_qtr_data["RSI"] = 0
                #Momemtum indicator - 2
                df_cmp_qtr_data["ROC"] = 0
                #volume indicator
                df_cmp_qtr_data["ADOSC"] = 0
                #volatility indicator
                df_cmp_qtr_data["ATR"] = 0
                #trend indicator
                df_cmp_qtr_data["EMA"] = 0
                
            comp_data = df_cmp_qtr_data.iloc[-1][["symbol", "sector", "RSI", "ROC", "ADOSC", "ATR", "EMA"]]     
            comp_data["start_date"] = quarter_list[idx-1]
            comp_data["end_date"] = quarter_list[idx]
            comp_list.append(comp_data)
        features_df = features_df.append(pd.DataFrame(comp_list))
    return (features_df)




def create_sharpe_returns(ohlcv_df):
    risk_free_rates = 0.0095
    quarter_list=["2018-04-01", "2018-07-01", "2018-10-01", "2019-01-01", "2019-04-01", "2019-07-01", "2019-10-01", "2020-01-01", "2020-04-01"]
    features_df = pd.DataFrame([])
    df_copy = ohlcv_df.copy()
    df_copy["time"] = pd.to_datetime(df_copy.time)
    for idx in range(1, len(quarter_list)):
        df_quarter_data = df_copy[(df_copy["time"] >= quarter_list[idx-1]) & (df_copy["time"]< quarter_list[idx])].reset_index()
        df_quarter_dataa = df_quarter_data[['adjusted_close','symbol', 'time','sector']]

        symbol_list = df_quarter_data["symbol"].unique()

        rf_rate = risk_free_rates

        df_quarter_data = df_quarter_data.sort_index(ascending=True, axis=0)

        sharpe_table = df_quarter_data[["adjusted_close", "time", "symbol"]].set_index("time")

        sharpe_table  = sharpe_table.pivot(columns='symbol')
        sharpe_table.columns = [col[1] for col in sharpe_table.columns]

        length_sharpe_table = len(sharpe_table)

        daily_ret = np.recarray((length_sharpe_table,), dtype=[(symbol, 'float') for symbol in symbol_list])

        # Initialize some arrays for storing data  
        average_returns = np.zeros(len(symbol_list))  
        return_stdev = np.zeros(len(symbol_list))  
        sharpe_ratio_formula = np.zeros(len(symbol_list))  

        for i in range(len(symbol_list)):

            daily_ret[symbol_list[i]] = sharpe_table[symbol_list[i]].pct_change().fillna(0)

            # Now that we have the daily returns in %, calculate the relevant stats.  
            average_returns[i] = np.mean(daily_ret[symbol_list[i]])  
            return_stdev[i] = np.std(daily_ret[symbol_list[i]])  

            sharpe_ratio_formula[i] = (average_returns[i] / return_stdev[i]) * np.sqrt(length_sharpe_table) 

        sr_df = pd.DataFrame()
        sr_df["symbol"] = symbol_list
        sr_df["start_date"] = quarter_list[idx-1]
        sr_df["end_date"] = quarter_list[idx]
        sr_df["Sharpe_ratio"] = sharpe_ratio_formula
        sr_df["average_returns"] = average_returns
        features_df = features_df.append(sr_df)
    return (features_df)    