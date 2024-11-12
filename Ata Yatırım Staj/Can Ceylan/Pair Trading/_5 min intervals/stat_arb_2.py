import pandas as pd
import datetime
import numpy as np
import csv
import psycopg2 as pg
from statsmodels.tsa.stattools import adfuller
import matplotlib.pyplot as plt
from sklearn import preprocessing
from arch.unitroot.cointegration import engle_granger
import os
import gc
    
def convert_to_candlestick(market_data, time_interval):

    market_data['timestamp'] = pd.to_datetime(market_data['timestamp'])


    market_data.set_index('timestamp', inplace=True)

    market_data['price'] = pd.to_numeric(market_data['price'], errors='coerce')
    market_data['quantity'] = pd.to_numeric(market_data['quantity'], errors='coerce')
    market_data['volume'] = market_data.price * market_data.quantity
    market_data['side'] = market_data['side'].apply(lambda x: 1 if x=='B' else( -1 if x=='S' else 0))
    market_data['volume_buy'] = market_data.volume * market_data.side
    market_data['volume_sell'] = market_data.volume * market_data.side
    market_data['volume_buy'] = market_data['volume_buy'].apply(lambda x: x if x > 0 else 0)
    market_data['volume_sell'] = market_data['volume_sell'].apply(lambda x: -x if x < 0 else 0)

    

    candlestick_data = market_data.resample(time_interval).agg({
        'price': 'ohlc',          # open, high, low, close
        'volume': 'sum',        # volume
        'volume_buy': 'sum',   
        'volume_sell': 'sum'

    })
    

    candlestick_data.reset_index(inplace=True)

    candlestick_data.columns = ['timestamp','open','high','low','close','volume','volume_buy','volume_sell'] 
    
    candlestick_data['open'] = candlestick_data['open']/1000
    candlestick_data['high'] = candlestick_data['high']/1000
    candlestick_data['low'] = candlestick_data['low']/1000
    candlestick_data['close'] = candlestick_data['close']/1000
    candlestick_data['volume'] = candlestick_data['volume']/1000
    candlestick_data['volume_buy'] = candlestick_data['volume_buy']/1000
    candlestick_data['volume_sell'] = candlestick_data['volume_sell']/1000
    

    candlestick_data['timestamp'] = pd.to_datetime(candlestick_data['timestamp'])
    candlestick_data.set_index('timestamp', inplace=True)

    filtered_df_1 = candlestick_data[candlestick_data.index.weekday < 5]
    
    
    start_time = pd.to_datetime('07:00:00').time()
    end_time = pd.to_datetime('14:59:00').time()
    filtered_df_1 = filtered_df_1.between_time(start_time, end_time)

    nan_days = filtered_df_1[filtered_df_1.index.time == pd.to_datetime('13:00:00').time()].isna().any(axis=1)
    nan_days = nan_days[nan_days].index.date.tolist()
    
    filtered_df_1 = filtered_df_1[~filtered_df_1.index.floor('D').isin(nan_days)]
    filtered_df_1.reset_index(inplace = True)
    
    return filtered_df_1



# Establish a connection
cnct = pg.connect(
    host="172.16.192.137",
    database="gtpbrdb",
    user="readuser",
    password="4y100K0ZO139rwxk",
    port="5432")


# print(datetime.datetime.now())
##########################################################################################

stock = 'AKBNK.E'

sql = '''SELECT x.* FROM public.trades_eq x WHERE symbol = \''''+ stock +'''\' and "date" > '2022-01-01'::date and "date" < '2023-06-01'::date order by "timestamp" ASC'''
df1 = pd.read_sql_query(sql,cnct)

df1.drop(['date'], inplace=True, axis=1)
df1.drop([0], inplace = True, axis = 0)  
df1.reset_index(inplace=True)
        
time_interval = '5T'

    
price_AKBNK = convert_to_candlestick(df1.copy(), time_interval)

del df1
gc.collect()

##########################################################################################

stock = 'YKBNK.E'

sql = '''SELECT x.* FROM public.trades_eq x WHERE symbol = \''''+ stock +'''\' and "date" > '2022-01-01'::date and "date" < '2023-06-01'::date order by "timestamp" ASC'''
df2 = pd.read_sql_query(sql,cnct)

df2.drop(['date'], inplace=True, axis=1)
df2.drop([0], inplace = True, axis = 0)  
df2.reset_index(inplace=True)

    
price_YKBNK = convert_to_candlestick(df2.copy(), time_interval)

del df2
gc.collect()

##########################################################################################

stock = 'ISCTR.E'

sql = '''SELECT x.* FROM public.trades_eq x WHERE symbol = \''''+ stock +'''\' and "date" > '2022-01-01'::date and "date" < '2023-06-01'::date order by "timestamp" ASC'''
df3 = pd.read_sql_query(sql,cnct)

df3.drop(['date'], inplace=True, axis=1)
df3.drop([0], inplace = True, axis = 0)  
df3.reset_index(inplace=True)
        
    
price_ISCTR = convert_to_candlestick(df3.copy(), time_interval)

del df3
gc.collect()

##########################################################################################

cnct.close()

###############


price_AKBNK.set_index('timestamp', inplace=True)
price_AKBNK = price_AKBNK.drop(columns = ['open', 'high','low','volume','volume_buy','volume_sell'])

price_YKBNK.set_index('timestamp', inplace=True)
price_YKBNK = price_YKBNK.drop(columns = ['open', 'high','low','volume','volume_buy','volume_sell'])

price_ISCTR.set_index('timestamp', inplace=True)
price_ISCTR = price_ISCTR.drop(columns = ['open', 'high','low','volume','volume_buy','volume_sell'])


output_dir = "P:\Can Ceylan\Pair Trading\5 min intervals\Data"
filename = os.path.join(output_dir, 'AKBNK.csv')
price_AKBNK.to_csv(filename, index=False)

output_dir = "P:\Can Ceylan\Pair Trading\5 min intervals\Data"
filename = os.path.join(output_dir, 'YKBNK.csv')
price_YKBNK.to_csv(filename, index=False)

output_dir = "P:\Can Ceylan\Pair Trading\5 min intervals\Data"
filename = os.path.join(output_dir, 'ISCTR.csv')
price_ISCTR.to_csv(filename, index=False)








def multiplier_caculator(z_score, entry_threshold):
    num = 10/3 * (-entry_threshold + z_score)/z_score
    
    if num < 0:
        num = 0
        
    return num





time_interval = 248
significance = 0.05
start_date = "2022-11-01"
end_date = "2023-06-30"





price_AKBNK = price_AKBNK.dropna()  # drop rows with nan values
price_AKBNK = price_AKBNK.drop(columns = ['Open', 'High','Low','Adj Close','Volume','Repaired?'])







extra_date = price_AKBNK.index.difference(price_YKBNK.index)
price_AKBNK = price_AKBNK.drop(extra_date)







log_price1 = np.log(price_AKBNK)
log_price2 = np.log(price_YKBNK)

log_price = log_price1.join(log_price2, lsuffix='_1', rsuffix='_2')




#For Testing
entry_list = [0.6, 0.5, 1, 1.2, 1.3]
exit_list = [0.15, 0.1]
stop_loss = [100]


# Perform pair trading
asset_list = ['AKBNK.E', 'YKBNK.E', 'ISCTR.E']
for entry_threshold in entry_list:
    for exit_threshold in exit_list:
        for stop_loss_threshold in stop_loss:
            
            myCSV = pd.DataFrame(columns = [ "Date_open", 'Date_close','Side','Hedge_Ratio','Multiplier','AKBNK_price_open',
                                             'AKBNK_price_close','YKBNK_price_open','YKBNK_price_close', 'Return (%)' ])
            

            prev_position = 0
            
            stock_AKBNK = 0
            stock_YKBNK = 0
            
            wallet = pd.DataFrame(columns=["Date","Value"])
            wallet.loc[0] = {"Date": price_AKBNK.index[time_interval], "Value": 100}
            
            money= 100
            count = 0
            
            row_count = -1
            money_before = 0
            while count+time_interval < len(log_price)-1:
                count += 1
                adf_result_A = adfuller(log_price.iloc[count:count+time_interval, 0])
                adf_statistic_A = adf_result_A[0]
                p_value_A = adf_result_A[1]

                # Perform ADF test for asset B
                adf_result_B = adfuller(log_price.iloc[count:count+time_interval, 1])
                adf_statistic_B = adf_result_B[0]
                p_value_B = adf_result_B[1]
                
                if p_value_A > significance and p_value_B > significance:
                
                    coint_result = engle_granger(log_price.iloc[count:count+time_interval, 0], log_price.iloc[count:count+time_interval, 1], trend="c", lags=0)
                    hedge_ratio = -coint_result.cointegrating_vector[1]
                    constant = coint_result.cointegrating_vector[2]
                    spread = log_price.iloc[count:count+time_interval, 0] - hedge_ratio * log_price.iloc[count:count+time_interval, 1] + constant
                    today_spread = log_price.iloc[count+time_interval, 0] - hedge_ratio * log_price.iloc[count+time_interval, 1] + constant
                    z_score = (today_spread - np.mean(spread)) / np.std(spread)
                    if coint_result.pvalue < significance:
                        plt.plot(spread)
                        multiplier = multiplier_caculator(z_score,entry_threshold)
                    
                        if z_score > entry_threshold and prev_position == 0:
                            # Short the overvalued stock and long the undervalued stock
                            stock_AKBNK -= multiplier
                            stock_YKBNK += hedge_ratio * multiplier
                            money_before = money
                            money += multiplier * (price_AKBNK.iloc[count+time_interval, 0]) - multiplier*(hedge_ratio * price_YKBNK.iloc[count+time_interval,0])
                            prev_position = 1
                            row_count += 1
                            
                            myCSV.loc[row_count, "Date_open"] = price_AKBNK.index[count+time_interval]
                            myCSV.loc[row_count, "Side"] = 'Sell'
                            myCSV.loc[row_count, "Hedge_Ratio"] = hedge_ratio
                            myCSV.loc[row_count, "Multiplier"] = multiplier
                            myCSV.loc[row_count, "AKBNK_price_open"] = price_AKBNK.iloc[count+time_interval, 0]
                            myCSV.loc[row_count, "ISCTR_price_open"] = price_YKBNK.iloc[count+time_interval,0]
                            
                            
                        elif z_score < -entry_threshold and prev_position == 0:
                            # Short the undervalued stock and long the overvalued stock
                            stock_AKBNK += multiplier
                            stock_YKBNK -= hedge_ratio * multiplier
                            money_before = money
                            money += -multiplier*(price_AKBNK.iloc[count+time_interval, 0]) + multiplier*(hedge_ratio * price_YKBNK.iloc[count+time_interval,0])
                            prev_position = -1
                            row_count += 1
                            
                            myCSV.loc[row_count, "Date_open"] = price_AKBNK.index[count+time_interval]
                            myCSV.loc[row_count, "Side"] = 'Buy'
                            myCSV.loc[row_count, "Hedge_Ratio"] = hedge_ratio
                            myCSV.loc[row_count, "Multiplier"] = multiplier
                            myCSV.loc[row_count, "AKBNK_price_open"] = price_AKBNK.iloc[count+time_interval, 0]
                            myCSV.loc[row_count, "ISCTR_price_open"] = price_YKBNK.iloc[count+time_interval,0]
                        
                if (abs(z_score) < exit_threshold or abs(z_score) > stop_loss_threshold) and prev_position != 0:
                    # Exit the position
                    money += (stock_AKBNK * price_AKBNK.iloc[count+time_interval, 0])+(stock_YKBNK * price_YKBNK.iloc[count+time_interval,0])
                    
                    stock_AKBNK = 0
                    stock_YKBNK = 0
                    prev_position = 0
                    
                    myCSV.loc[row_count, "Date_close"] = price_AKBNK.index[count+time_interval]
                    myCSV.loc[row_count, "AKBNK_price_close"] = price_AKBNK.iloc[count+time_interval, 0]
                    myCSV.loc[row_count, "ISCTR_price_close"] = price_YKBNK.iloc[count+time_interval,0]
                    myCSV.loc[row_count, "Return (%)"] = (money/money_before - 1) * 100
                
                wallet.loc[len(wallet)] = {"Date": price_AKBNK.index[count+time_interval], "Value": money + ((stock_AKBNK * price_AKBNK.iloc[count+time_interval, 0]) + (stock_YKBNK * price_YKBNK.iloc[count+time_interval, 0]))}
            
                    
            
            # Plot wallet
            plt.figure(figsize=(10, 6))
            plt.plot(wallet["Date"], wallet["Value"])
            plt.xlabel("Date")
            plt.ylabel("Money")
            plt.title("Money over Time")
            plt.xticks(rotation=45)
            plt.grid(True)
            plt.show()
            
            Total_Return = ((wallet['Value'].iat[-1])/100 - 1) * 100
        
        
            output_dir = "P:\Can Ceylan\Pair Trading\5 min intervals\Tests"
            filename = os.path.join(output_dir, f'AKBNK_ISCTR_{Total_Return:.2f}_{entry_threshold:.2f}-{exit_threshold:.2f}-{stop_loss_threshold:.2f}.csv')
            myCSV.to_csv(filename, index=False)



















