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
    
    #Variable dictionary
    variable_dict = {}
    
    #Output directory
    output_dir = "P:\Can Ceylan\HFT\_1 second intervals\Data"
    
    readData(clusters, variable_dict, output_dir)
    
    critical_values = [0.5] # in percentage
    intervals = [0.1,0.2,0.3,0.4,0.5] # in seconds
   
    
    #Main loop
    # for acceptance in acceptances:
        
    myDataFrames = {}
    for critical_value in critical_values:
        for interval in intervals:
            for cluster in clusters:
                for asset1 in cluster:
                    for asset2 in cluster:
                        if asset1 != asset2:
                            myDataFrames[(asset1,asset2,critical_value,interval)] = pd.DataFrame(columns= ['timestamp', f'{asset1}_price_change', f'{asset2}_price_change'])
    
    for cluster in clusters:
        for i in range(len(cluster)):
            
            asset1 = cluster[i]
            grouped = variable_dict[asset1].groupby('timestamp')
            
            for timestamp, group in grouped:
                
                percentage_change1 = ((group['price'].max() / group['price'].min()) - 1) * 100
                
                for critical_value in critical_values:
                    if percentage_change1 >= critical_value:
            
                        first_price1 = group['price'].iloc[0]
                        last_price1 = group['price'].iloc[-1]
                        
                        if last_price1 > first_price1:
                            price_movement1 = "Increased"
                        elif last_price1 < first_price1:
                            price_movement1 = "Decreased"
                        else:
                            price_movement1 = 'NA'
                            
                        if price_movement1 == 'Increased':
                            for asset2 in cluster:
                                if asset1 != asset2:
                                    for interval in intervals:
                                        
                                        start_time = pd.Timestamp(timestamp)
                                        end_time = timestamp + pd.Timedelta(seconds=interval)
                                        selected_rows = variable_dict[asset2][(variable_dict[asset2].index >= start_time) & (variable_dict[asset2].index <= end_time)]
                                        
                                        if selected_rows.size != 0:
                                            percentage_change2 = ((selected_rows['price'].max() / selected_rows['price'].min()) - 1) * 100
                                            
                                            first_price2 = selected_rows['price'].iloc[0]
                                            last_price2 = selected_rows['price'].iloc[-1]
                                            
                                            if last_price2 > first_price2:
                                                price_movement2 = "Increased"
                                            elif last_price2 < first_price2:
                                                price_movement2 = "Decreased"
                                            else:
                                                price_movement2 = 'NA'
                                
                                            # Calculating percentage change.
                                            if price_movement2 == 'Decreased':
                                                percentage_change2 *= -1
                                            
                                            myData = {
                                                'timestamp' : timestamp,
                                                f'{asset1}_price_change' : percentage_change1,
                                                f'{asset2}_price_change' : percentage_change2
                                                }
                                            
                                            new_row = pd.DataFrame([myData], index=['timestamp'])
                                            
                                            myDataFrames[(asset1,asset2,critical_value,interval)] = pd.concat([myDataFrames[(asset1,asset2,critical_value,interval)], new_row], ignore_index=True)
                                        else:
                                            myData = {
                                                'timestamp' : timestamp,
                                                f'{asset1}_price_change' : percentage_change1,
                                                f'{asset2}_price_change' : pd.Series(dtype=float),
                                                }
                                            
                                            new_row = pd.DataFrame([myData], index=['timestamp'])
                                            
                                            myDataFrames[(asset1,asset2,critical_value,interval)] = pd.concat([myDataFrames[(asset1,asset2,critical_value,interval)], new_row], ignore_index=True)
                                            
    
    for critical_value in critical_values:
        for interval in intervals:
            for cluster in clusters:
                for asset1 in cluster:
                    for asset2 in cluster:
                        if asset1 != asset2:
                            file_path = f"P:\Can Ceylan\HFT\_1 second intervals\Results2\{asset1}_{asset2}_{critical_value}_{interval}.csv"
                            myDataFrames[(asset1,asset2,critical_value,interval)].to_csv(file_path)

                            
                        
        
                
        
    
    
    


















