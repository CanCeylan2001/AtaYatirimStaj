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

def pct_change_min_max(x,asset):
    if x.empty:
        return pd.Series(dtype=float)
    min_value = x[asset].min()
    max_value = x[asset].max()

    if min_value == max_value:
        return 0
    print('mrb')
    pct_change = (100 * ((max_value / min_value) - 1))
    df_x = x.reset_index()
    a = df_x[asset].idxmin()
    b = df_x[asset].idxmax()
    
    if b>a:
        return pct_change
    return -pct_change
        

if __name__ == "__main__":

    #Cluster list
    clusters = [['AKBNK.E', 'YKBNK.E', 'ISCTR.E'], ['FROTO.E', 'TOASO.E'], ['KCHOL.E', 'SAHOL.E'], ['KOZAA.E', 'KOZAL.E', 'IPEKE.E']]
    
    #Variable dictionary
    variable_dict = {}
    
    #Output directory
    output_dir = "P:\Can Ceylan\HFT\_1 second intervals\Data"
    
    readData(clusters, variable_dict, output_dir)
    
    unique_assets = []
    for cluster in clusters:
        for asset in cluster:
            unique_assets.append(asset)
            variable_dict[asset].rename(columns={'price': asset}, inplace=True)
    
##############################################################################################################
    
    groups_change = variable_dict[clusters[0][0]].resample('1S').apply(lambda x: pct_change_min_max(x, clusters[0][0]))
    
    for assets in clusters:
        for asset in assets:
            if asset != clusters[0][0]:
                grouped_data = variable_dict[asset].resample('1S').apply(lambda x: pct_change_min_max(x, asset))
                groups_change = pd.merge_asof(groups_change, grouped_data, on='timestamp')
        
    groups_change.set_index('timestamp')
    
    groups_min = variable_dict[clusters[0][0]].resample('1S').agg({clusters[0][0]: 'min'}, min_periods=1)
    
    for assets in clusters:
        for asset in assets:
            if asset != clusters[0][0]:
                grouped_data = variable_dict[asset].resample('1S').agg({f'{asset}': 'min'}, min_periods=1)
                groups_min = pd.merge_asof(groups_min, grouped_data, on='timestamp')
                
    groups_min.set_index('timestamp')


    groups_max = variable_dict[clusters[0][0]].resample('1S').agg({clusters[0][0]: 'max'}, min_periods=1)
    
    for assets in clusters:
        for asset in assets:
            if asset != clusters[0][0]:
                grouped_data = variable_dict[asset].resample('1S').agg({f'{asset}': 'max'}, min_periods=1)
                groups_max = pd.merge_asof(groups_max, grouped_data, on='timestamp')

    groups_max.set_index('timestamp')
##############################################################################################################
      
    critical_values = [0.3, 0.5] # in percentage
    intervals = [1,5] # in seconds
    #acceptances = [0] #percentage change in prices for asset2 to be considered success
    
    for critical_value in critical_values:
        exceeds_threshold = groups_change >= critical_value
        for cluster in clusters:
            for asset1 in cluster:
                other_assets = [asset2 for cluster2 in clusters for asset2 in cluster2 if asset2 != asset1]
                timestamps = groups_change.index[exceeds_threshold[asset1]]
                
                for interval in intervals:
                    
                    myCSV = pd.DataFrame(columns= ['timestamp', asset1] + [f'{asset}' for asset in other_assets])
                    
                    def min_interval_data(ts):
                        end_time = ts + pd.Timedelta(seconds=interval)
                        interval_data = groups_min.loc[ts:end_time, other_assets].copy()
                        min_value = interval_data.min()
                        return min_value
                    
                    interval_data_min = [min_interval_data(ts) for ts in timestamps]
                    
                    def max_interval_data(ts):
                        end_time = ts + pd.Timedelta(seconds=interval)
                        interval_data = groups_max.loc[ts:end_time, other_assets].copy()
                        max_value = interval_data.max()
                        return max_value
                    
                    interval_data_max = [max_interval_data(ts) for ts in timestamps]
                    
                    price_changes = ((interval_data_max / interval_data_min)-1)*100
                    
                    #write data to myCSV
                    #handle NaN / missing values
                    
                    
                    
                    
                    
                    
                    
                    
            
            
            
            
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    