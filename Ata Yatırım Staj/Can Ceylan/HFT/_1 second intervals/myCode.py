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
    output_dir = "P:\Can Ceylan\Pair Trading\_1 second intervals\Data"
    
    readData(clusters, variable_dict, output_dir)
    
    critical_values = [0.3] # in percentage
    intervals = [1] # in seconds
    acceptances = [0] #percentage change in prices for asset2 to be considered success
                
    #Main loop
    for acceptance in acceptances:
        for critical_value in critical_values:
            for interval in intervals:
                success_percentage_dict = {}
                for cluster in clusters:
                    for i in range(len(cluster)):
                        for k in range(i+1,len(cluster)):
                            key = (cluster[i], cluster[k])
                            success_percentage_dict[key] = [0,0,0]
                                    
                        asset1 = cluster[i]
                        
                        grouped = variable_dict[asset1].groupby('timestamp')
                        
                        for timestamp, group in grouped:
                            
                            percentage_change1 = ((group['price'].max() / group['price'].min()) - 1) * 100
                            
                            if percentage_change1 > critical_value:
                                
                                df_reset = group.reset_index()
                                
                                first_price1 = df_reset['price'].idxmin()
                                if isinstance(first_price1, int) == False:
                                    first_price1 = int(first_price1.iloc[0])
                                    
                                last_price1 = df_reset['price'].idxmax()
                                if isinstance(last_price1, int) == False:
                                    last_price1 = int(last_price1.iloc[0])
                                
                                if last_price1 > first_price1:
                                    price_movement1 = "Increased"
                                elif last_price1 < first_price1:
                                    price_movement1 = "Decreased"
                                else:
                                    price_movement1 = 'NA'
                                    
                                if price_movement1 != 'NA' and percentage_change1 > critical_value:
                                    
                                    for j in range(i+1,len(cluster)):
                                        asset2 = cluster[j]
                                            
                                        key = (asset1,asset2)
                                        success_percentage_dict[key][0] = success_percentage_dict[key][0] + 1
                                        
                                        start_time = pd.Timestamp(timestamp)
                                        end_time = timestamp + pd.Timedelta(seconds=interval)
                                        selected_rows = variable_dict[asset2][(variable_dict[asset2].index >= start_time) & (variable_dict[asset2].index <= end_time)]
                                        
                                        if selected_rows.size != 0:
                                            percentage_change2 = ((selected_rows['price'].max() / selected_rows['price'].min()) - 1) * 100
                                            
                                            df_reset2 = selected_rows.reset_index()
                                            
                                            first_price2 = df_reset2['price'].idxmin()
                                            if isinstance(first_price2, int) == False:
                                                first_price2 = int(first_price2.iloc[0])
                                                
                                            last_price2 = df_reset2['price'].idxmax()
                                            if isinstance(last_price2, int) == False:
                                                last_price2 = int(last_price2.iloc[0])
                                            
                                            if last_price2 > first_price2:
                                                price_movement2 = "Increased"
                                            elif last_price2 < first_price2:
                                                price_movement2 = "Decreased"
                                            else:
                                                price_movement2 = 'NA'
                                                
                                            if price_movement1 == price_movement2 and percentage_change2 > acceptance:
                                                success_percentage_dict[key][1] += 1
                                    
                # Calculating success percentage.
                for item in success_percentage_dict:
                    if success_percentage_dict[item][0] != 0:
                        success_percentage_dict[item][2] = (success_percentage_dict[item][1] / success_percentage_dict[item][0]) * 100
                    
                file_path = f"P:\Can Ceylan\Pair Trading\_1 second intervals\Results_{critical_value}_{interval}_{acceptance}.txt"
                with open(file_path, 'w') as file:
                    for item in success_percentage_dict:
                        text = str(item) + ' : ' + str(success_percentage_dict[item]) + '\n'
                        file.write(text)
                                    
                            
                            
            
            
    





















