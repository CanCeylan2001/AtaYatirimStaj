import datetime
import pandas as pd
import numpy as np

# List of BIST-30 stock symbols
bist30_symbols = [
    "THYAO",
    "TUPRS",
    "BIMAS",
    "SISE",
    "SASA",
    "AKBNK",
    "KCHOL",
    "EREGL",
    "TCELL",
    "FROTO",
    "SAHOL",
    "ISCTR",
    "YKBNK",
    "ASELS",
    "PGSUS",
    "TOASO",
    "HEKTS",
    "GARAN",
    "KOZAL",
    "ENKAI",
    "GUBRF",
    #"ASTOR",
    "PETKM",
    "TAVHL",
    "EKGYO",
    "KRDMD",
    "ARCLK",
    "ALARK",
    "ODAS",
    "KOZAA"
]

# Define a custom aggregation function to enforce data type
def enforce_dtype(series):
    return series.astype(float).sum()  # Convert to float and then sum

custom_column_names = ["Saat", "Hisse", "Fiyat", "Adet", "Alan", "Satan", "drop1", "drop2", "Hacim", "drop3", "drop4", "BIST_No"]
# Drop columns by name
columns_to_drop = ["drop1", "drop2", "drop3", "drop4", "BIST_No"]  # List of column names to drop

df = pd.read_csv("D:/Ata Yatırım Staj/Can Ceylan/Asset_Broker Interface yay me/all_trades.csv",sep=';',header=None, names=custom_column_names, index_col=None)
df.drop(columns=columns_to_drop, inplace=True)
df["Hacim"] = df["Hacim"].str.replace(',', '').astype(float)

# Filter the DataFrame based on the Series values using isin()
filtered_df = df[df["Hisse"].isin(bist30_symbols)]
filtered_df.set_index('Saat', inplace=True)
filtered_df.sort_index(inplace=True)

# # get duplicated values as float and replace 0 with NaN
# values = filtered_df.index.duplicated(keep=False).astype(float)
# values[values==0] = np.NaN

# missings = np.isnan(values)
# cumsum = np.cumsum(~missings)
# diff = np.diff(np.concatenate(([0.], cumsum[missings])))
# values[missings] = -diff

# # print result
# result = filtered_df.index + np.cumsum(values).astype(np.timedelta64(1,'ns'))

# timedelta_value = pd.to_timedelta(result)
# reference_date = pd.to_datetime('2023-08-22')
result_datetime = pd.to_timedelta(filtered_df.index)

filtered_df.set_index(result_datetime, inplace=True)
filtered_df.drop(columns=['Fiyat'], inplace=True)
filtered_df = filtered_df.rename_axis('Timestamp')
    
# Create a MultiIndex
index = pd.MultiIndex.from_tuples([], names=('timestamp', 'asset', 'company'))

# Create an empty DataFrame with the MultiIndex
columns = ['Net Volume']
main_dataframe = pd.DataFrame(index=index, columns=columns)

resampled_df = filtered_df.resample('1T')
for timestamp, group_df in resampled_df:    
    grouped_buyers = group_df.groupby(['Hisse', 'Alan'])
    grouped_sellers = group_df.groupby(['Hisse', 'Satan'])

    # Calculate Buy and Sell volumes
    grouped_buyers_volumes = grouped_buyers['Hacim'].sum().unstack(level='Hisse').fillna(0)
    grouped_sellers_volumes = grouped_sellers['Hacim'].sum().unstack(level='Hisse').fillna(0)
    
    unique_names = set(grouped_buyers_volumes.index).union(grouped_sellers_volumes.index)
    
    aligned_buyers_volumes = grouped_buyers_volumes.reindex(index=unique_names)
    aligned_sellers_volumes = grouped_sellers_volumes.reindex(index=unique_names)
    aligned_buyers_volumes.fillna(0, inplace=True)
    aligned_sellers_volumes.fillna(0, inplace=True)

    # Calculate Net Volume
    net_volumes = aligned_buyers_volumes - aligned_sellers_volumes

    # Create a new DataFrame for the current timestamp's net_volumes
    net_volume_df = pd.DataFrame(net_volumes.stack(), columns=['Net Volume'])

    # Set the timestamp for net_volume_df
    net_volume_df['timestamp'] = timestamp

    net_volume_df.reset_index(inplace=True)
    net_volume_df.set_index(['timestamp', 'Hisse', 'Alan'], inplace = True)

    main_dataframe = pd.concat([main_dataframe, net_volume_df], ignore_index=False, sort=False)


parquet_filepath = "D:/Ata Yatırım Staj/Can Ceylan/Asset_Broker Interface yay me/todays_parsed_data.parquet"
csv_fiepath= "D:/Ata Yatırım Staj/Can Ceylan/Asset_Broker Interface yay me/todays_parsed_data.csv"
new_frame = main_dataframe[main_dataframe["Net Volume"] != 0]
new_frame.index = new_frame.index.set_names(['Timestamp', 'Asset', 'Firm'])

df_unstacked = new_frame.unstack(level='Firm')
df_unstacked.columns = [col[1] for col in df_unstacked.columns]
df_unstacked.columns = df_unstacked.columns.str.strip()
df_unstacked.fillna(0, inplace=True)

df_unstacked.to_parquet(parquet_filepath, index=True)
df_unstacked.to_csv(csv_fiepath, index=True)




