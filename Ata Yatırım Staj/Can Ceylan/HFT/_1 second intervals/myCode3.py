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
import pyarrow.parquet as pq

##########################################################################################

#Getting the data from the database and parquetting it for future runs.
def getData(clusters):
    for cluster in clusters:
        for asset in cluster:
            
            # Establish a connection
            cnct = pg.connect(
                host="172.16.192.137",
                database="gtpbrdb",
                user="readuser",
                password="4y100K0ZO139rwxk",
                port="5432")
        
            stock = asset
            
            sql = '''SELECT x.* FROM public.trades_eq x WHERE symbol = \''''+ stock +'''\' and "date" > '2022-01-01'::date and "date" < '2023-06-01'::date order by "timestamp" ASC'''
            dataframe = pd.read_sql_query(sql,cnct)
            
            dataframe.drop(['date'], inplace=True, axis=1)
            dataframe.drop([0], inplace = True, axis = 0)  
            dataframe.reset_index(inplace=True)
            
            dataframe.set_index('timestamp', inplace=True)
            dataframe = dataframe.drop(columns = ['index', 'symbol','quantity', 'side'])
            
            filename = os.path.join(output_dir, (asset[:-2] + 'parquet'))
            dataframe.to_parquet(filename, index=True)
            
            del dataframe
            gc.collect()
    
            cnct.close()

##########################################################################################

#Getting data from the parquetted files.
def readData(clusters, variable_dict, output_dir):
    for cluster in clusters:
        for asset in cluster:
            filename = os.path.join(output_dir, (asset[:-2] + '.parquet'))
            dataframe = pd.read_parquet(filename)
            
            # Update the variable_dict with the new dataframe
            variable_dict[asset] = dataframe.copy()

##########################################################################################   

if __name__ == "__main__":

    #Cluster list
    clusters = [['AKBNK.E', 'YKBNK.E', 'ISCTR.E'], ['FROTO.E', 'TOASO.E'], ['KCHOL.E', 'SAHOL.E'], ['KOZAA.E', 'KOZAL.E', 'IPEKE.E']]
    clusters = [['AKBNK.E', 'YKBNK.E', 'ISCTR.E']]
    
    
    #Variable dictionary
    variable_dict = {}
    
    #Output directory
    output_dir = "P:\Can Ceylan\HFT\_1 second intervals\Data"
    
    readData(clusters, variable_dict, output_dir)
    
    critical_values = [(0.3 , 0.2), (0.5, 0.3)] # asset1_price_change_threshold, asset2_exit_threshold
    intervals = [1, 5] # in seconds
  
    
  
    myDataFrames = {}
    for critical_value, threshold in critical_values:
        for interval in intervals:
            for cluster in clusters:
                for asset1 in cluster:
                    for asset2 in cluster:
                        if asset1 != asset2:
                            myDataFrames[(asset1,asset2,critical_value,interval)] = pd.DataFrame(columns= 
            ['timestamp', f'{asset1}_price_change', f'{asset2}_entry_price', '..seconds_later', f'{asset2}_exit_price','return'])
                            
    for cluster in clusters:
        for asset1 in cluster:
            start_time = pd.Timestamp("06:50:00")
            end_time = pd.Timestamp("06:59:59")
            variable_dict[asset1] = variable_dict[asset1][~((variable_dict[asset1].index.time >= start_time.time()) & (variable_dict[asset1].index.time <= end_time.time()))]
    
            start_time = pd.Timestamp("14:59:01")
            end_time = pd.Timestamp("15:10:00")
            variable_dict[asset1] = variable_dict[asset1][~((variable_dict[asset1].index.time >= start_time.time()) & (variable_dict[asset1].index.time <= end_time.time()))]
           
            variable_dict[asset1] = variable_dict[asset1].reset_index()
            
            
    count =0
    
    for cluster in clusters:
        for i in range(len(cluster)):
            
            asset1 = cluster[i]
            grouped = variable_dict[asset1].iloc[1:].groupby('timestamp')
            
            for timestamp, group in grouped:
                
                print(0)
                row_previous = variable_dict[asset1][variable_dict[asset1]['timestamp'] < timestamp]
                if not row_previous.empty:
                    row_previous = row_previous.iloc[-1]
                else:
                    row_previous = variable_dict[asset1].iloc[0]
                
                
                group = pd.concat([row_previous, group], ignore_index=True)
                
                percentage_change1 = ((group['price'].max() / group['price'].min()) - 1) * 100
                print(1)
                for critical_value, threshold in critical_values:
                    if percentage_change1 >= critical_value:
                        
                        first_price1 = group['price'].idxmin()
                        last_price1 = group['price'].idxmax()
                        
                        if last_price1 > first_price1:
                            price_movement1 = "Increased"
                        # elif last_price1 < first_price1:
                        #     price_movement1 = "Decreased"
                        else:
                            price_movement1 = 'NA'
                        print(price_movement1)
                        if price_movement1 == 'Increased':
                            for asset2 in cluster:
                                if asset1 != asset2:
                                    row_previous2 = variable_dict[asset2][variable_dict[asset2]['timestamp'] < timestamp]
                                    if not row_previous2.empty:
                                        row_previous2 = row_previous2.iloc[-1]
                                    else:
                                        row_previous2 = variable_dict[asset2].iloc[0]
                                    for interval in intervals:
                                        print(4)
                                        print(count)
                                        count +=1
                                        start_time = pd.Timestamp(timestamp)
                                        end_time = timestamp + pd.Timedelta(seconds=interval)
                                        selected_rows = variable_dict[asset2][(variable_dict[asset2]['timestamp'] >= start_time) & (variable_dict[asset2]['timestamp'] <= end_time)]
                                        
                                        selected_rows = pd.concat([row_previous2, selected_rows], ignore_index = True)
        
                                        exit_time = 5
                                        print(5)
                                        if selected_rows.size > 1:
                                            
                                            print(6)
                                            entry_price = selected_rows['price'].iloc[0]
                                            exit_price = selected_rows['price'].iloc[-1]
                                            
                                            upper_exit_price = entry_price * (1 + threshold/100)
                                            lower_exit_price = entry_price * (1 - threshold/100)
                                            
                                            # for timestamp2, price in selected_rows:
                                            #     if price <= lower_exit_price or price >= upper_exit_price:
                                            #         exit_price = price
                                            #         exit_time = ((timestamp2 - timestamp).microseconds) / 1000000
                                            #         break
                                                
                                            exit_condition = ((selected_rows['price'] <= lower_exit_price) | (selected_rows['price'] >= upper_exit_price))
                                            exit_index = selected_rows[exit_condition].idxmax()
                                            
                                            exit_price = selected_rows.loc[exit_index, 'price']
                                            exit_time = ((selected_rows.loc[exit_index, 'timestamp'] - timestamp).microseconds) / 1000000
                                            
                                            print(7)
                                            ##################################
                                            
                                            # percentage_change2 = ((selected_rows['price'].max() / selected_rows['price'].min()) - 1) * 100
                                            
                                            # df = selected_rows.reset_index()
                                            
                                            # first_price2 = df['price'].idxmin()
                                            # last_price2 = df['price'].idxmax()
                                                
                                            # if last_price2 > first_price2:
                                            #     price_movement2 = "Increased"
                                            # elif last_price2 < first_price2:
                                            #     price_movement2 = "Decreased"
                                            # else:
                                            #     price_movement2 = 'NA'
                                
                                            # # Calculating percentage change.
                                            # if price_movement2 == 'Decreased':
                                            #     percentage_change2 *= -1
                                            
                                            return_rate = ((exit_price / entry_price) -1)*100
                                            myData = {
                                                'timestamp' : timestamp,
                                                f'{asset1}_price_change' : percentage_change1,
                                                f'{asset2}_entry_price' : entry_price,
                                                '..seconds_later' : exit_time,
                                                f'{asset2}_exit_price' : exit_price,
                                                'return' : return_rate
                                                }
                                            
                                            new_row = pd.DataFrame([myData], index=['timestamp'])
                                            
                                            myDataFrames[(asset1,asset2,critical_value,interval)] = pd.concat([myDataFrames[(asset1,asset2,critical_value,interval)], new_row], ignore_index=True)
                                            
                                        else:
                                            myData = {
                                                'timestamp' : timestamp,
                                                f'{asset1}_price_change' : percentage_change1,
                                                f'{asset2}_entry_price' : entry_price,
                                                '..seconds_later' : 5,
                                                f'{asset2}_exit_price' : entry_price,
                                                'return' : 0
                                                }
                                            
                                            new_row = pd.DataFrame([myData], index=['timestamp'])
                                            
                                            myDataFrames[(asset1,asset2,critical_value,interval)] = pd.concat([myDataFrames[(asset1,asset2,critical_value,interval)], new_row], ignore_index=True)
                                            
    
    
    
    for critical_value, threshold in critical_values:
        for interval in intervals:
            for cluster in clusters:
                for asset1 in cluster:
                    for asset2 in cluster:
                        if asset1 != asset2:
                            money = ((myDataFrames[(asset1,asset2,critical_value,interval)]['return']/100 + 1 ).prod() -1)
                            file_path = f"P:\Can Ceylan\HFT\_1 second intervals\Test_Results\{asset1}_{asset2}_{money:.2f}{critical_value:.1f}_{threshold:.1f}_{interval:.0f}.csv"
                            myDataFrames[(asset1,asset2,critical_value,interval)].set_index('timestamp',inplace=True)
                            myDataFrames[(asset1,asset2,critical_value,interval)].to_csv(file_path, index=True, index_label='timestamp', date_format='%Y-%m-%d_%H:%M:%S.%f')

                            
                       