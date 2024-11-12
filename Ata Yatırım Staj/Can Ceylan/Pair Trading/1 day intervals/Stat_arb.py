from sklearn.decomposition import PCA
from statsmodels.tsa.stattools import adfuller
import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import DBSCAN
from sklearn import preprocessing
# from sklearn.neighbors import NearestNeighbors
# from kneed import KneeLocator
# import plotly.express as px
# from statsmodels.tsa.stattools import coint
from arch.unitroot.cointegration import engle_granger
import os


def multiplier_caculator(z_score, entry_threshold):
    br = abs(z_score)
    num = 2*(1-br)/br + 1.2
    
    if num < 0:
        num = 0
    return 1
        





# Define the ticker symbols
#XU030 has 1506 price info on excel, 1495 on yfinance, while other stocks all have 1534. Decided to skip XU030.
tickerStrings = ['AKBNK.IS', 'YKBNK.IS', 'GARAN.IS', 'ISCTR.IS', 'THYAO.IS', 'PGSUS.IS', 'TAVHL.IS', 'ASELS.IS', 'EREGL.IS', 
                 'KRDMD.IS', 'PETKM.IS', 'TUPRS.IS', 'FROTO.IS', 'TOASO.IS', 'KCHOL.IS', 'DOHOL.IS', 'SAHOL.IS', 'TTKOM.IS', 'TCELL.IS',
                 'EKGYO.IS', 'OYAKC.IS', 'BIMAS.IS','MGROS.IS', 'KOZAA.IS', 'KOZAL.IS', 'IPEKE.IS']

# # Start and end dates for the data
# myDates = ["2017-01-01","2018-01-01", "2019-01-01","2020-01-01","2021-01-01","2022-01-01", "2022-12-31"]
# output_dir = "P:\Can Ceylan\Pair Trading"

# for i in range(len(myDates)-1):
#     start_date = myDates[i]
#     end_date = myDates[i+1]
#     filename = os.path.join(output_dir, f"{start_date}_{end_date}.txt")
#     with open(filename, 'w') as file:
    
    
#         # Fetch the data from Yahoo Finance
        
#         df_list = list()
#         for ticker in tickerStrings:
#             data = yf.download(ticker, group_by="Ticker", start=start_date, end=end_date, repair=True)
#             data = data.dropna()  # drop rows with nan values
#             data = data.drop(columns = ['Open', 'High','Low','Close','Volume','Repaired?'])
#             df_list.append(data)
            
        
#         # df_xu030 = pd.read_excel('C:/Users/canc/Downloads/xu030daily.xlsx', parse_dates=['Date'], index_col='Date')
#         # df_xu030 = df_xu030.drop(columns = ['Time', 'Open','High', 'Low'])
            
            
        
#         return_list = pd.DataFrame()
        
#         for i in range(len(tickerStrings)):
#             close_price = df_list[i]['Adj Close']
#             returns = close_price.pct_change().iloc[1:]
#             return_list[i] = returns
        
        
#         #initiating PCA
#         pca = PCA()
#         pca.fit(return_list)
        
        
#         components = [str(x + 1) for x in range(pca.n_components_)]
#         explained_variance_pct = pca.explained_variance_ratio_ * 100
        
#         plt.figure(figsize=(15, 10))
#         plt.bar(components, explained_variance_pct)
#         plt.title("Ratio of Explained Variance")
#         plt.xlabel("Principle Component #")
#         plt.ylabel("%")
#         plt.show()
        
#         #From the plot we see that the first component captures about 40% of the data, the second captures about 10%
#         #while the remaining components capture less than 10%. This means we do not have much similarity between all
#         #the instruments, but we still may have instrument pairs that act similarly.
        
        
#         #Kaiser rule: pick all components that have eigenvalues greater than 1.
#         # count=0
#         # for num in pca.explained_variance_:
#         #     if num>1:
#         #         count += 1
#         #     else:
#         #         break
#         # This yields 0 components unfortunately, disregard this part.
        
        
#         # Since there is usually a sharp decline at the start, we always get about 3 components, and lose too much info.
#         # # Use the KneeLocator to determine the optimal number of components
#         # kneedle = KneeLocator(range(1, pca.n_components_ + 1), explained_variance_pct, curve="convex", direction="decreasing")
#         # optimal_n_components = kneedle.knee

#         # file.write(f"Optimal number of components: {optimal_n_components}\n")
        
#         pca = PCA(n_components= 7)
#         pca.fit(return_list)
        
        
#         X = pca.components_.T
#         X = preprocessing.StandardScaler().fit_transform(X)
        
#         # ##################
#         # neighbors = 6
#         # # X_embedded is your data
#         # nbrs = NearestNeighbors(n_neighbors=neighbors ).fit(X)
#         # distances, indices = nbrs.kneighbors(X)
#         # distance_desc = sorted(distances[:,len(X)-1], reverse=True)
        
#         # kneedle = KneeLocator(range(1,len(distanceDec)+1),  #x values
#         #                       distanceDec, # y values
#         #                       S=1.0, #parameter suggested from paper
#         #                       curve="convex", #parameter from figure
#         #                       direction="decreasing") #parameter from figure
        
#         # ##################
        
#         # Gives Too Large epsilon value, such that everything falls within the same cluster.
#         # # Use the KneeLocator to determine the optimal epsilon value for DBSCAN
#         # nbrs = NearestNeighbors(n_neighbors=2).fit(X)
#         # distances, indices = nbrs.kneighbors(X)
#         # distances = np.sort(distances, axis=0)
#         # kneedle_epsilon = KneeLocator(range(1, len(distances) + 1), distances[:, 1], S=1.0, curve="concave", direction="increasing").knee
        

#         # file.write(f"Optimal epsilon value: {kneedle.knee_y}\n")

#         # Perform DBSCAN Clustering with the optimal epsilon value
#         clf = DBSCAN(eps=0.9, min_samples=2)
#         clf.fit(X)
#         labels = clf.labels_
#         n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
#         file.write(f"Clusters discovered: {n_clusters_}\n")
        
        
#         #Extract clusters found by DBSCAN
        
#         cluster_list = []
#         for i in range(n_clusters_):
#             cluster_list.append([])
        
#         for j in range(len(labels)):
#             if labels[j] != -1:
#                 cluster_list[labels[j]].append(tickerStrings[j])
        
#         for i in range(len(cluster_list)):
#             file.write(f"Cluster {i + 1}: {cluster_list[i]}\n")
        
        
#         # ##Cointegration tests for all pairs in clusters.
#         # significance = 0.05
#         # cointegrated_pair_list = []
#         # cointegrated_pair_score_list = []
#         # for i in cluster_list:
#         #     for j in range(len(i)-1):
#         #         for k in range(j+1,len(i)):
#         #             asset1 = i[j]
#         #             asset2 = i[k]
                    
#         #             price1 = df_list[tickerStrings.index(asset1)]['Adj Close']
#         #             price2 = df_list[tickerStrings.index(asset2)]['Adj Close']
                    
#         #             log_price1 = np.log(price1)
#         #             log_price2 = np.log(price2)
                    
#         #             coint_result = coint(log_price1, log_price2)
#         #             score = coint_result[0]
#         #             pvalue = coint_result[1]
                    
#         #             if pvalue < significance: ##reject null hypo, which implied series has no cointegration.
#         #                 cointegrated_pair_list.append([asset1, asset2])
#         #                 cointegrated_pair_score_list.append(score)
                        
#         #             print(asset1, '-', asset2, ': p value =', pvalue, score)
        
        
        
#         ##Other way of cointegration test I guess:
#         significance = 0.05
#         cointegrated_pair_list = []
#         #For every pair in every cluster:
#         for i in cluster_list:
#             for j in range(len(i)-1):
#                 for k in range(j+1,len(i)):
#                     asset1 = i[j]
#                     asset2 = i[k]
                    
#                     #Retrieving price information.
#                     price1 = df_list[tickerStrings.index(asset1)]['Adj Close']
#                     price2 = df_list[tickerStrings.index(asset2)]['Adj Close']
                    
#                     log_price1 = np.log(price1)
#                     log_price2 = np.log(price2)
                    
#                     log_price1 = log_price1.to_frame()
#                     log_price2 = log_price2.to_frame()
                    
#                     log_price = log_price1.join(log_price2, lsuffix='_1', rsuffix='_2')
                    
                    
#                     #ADF on both assets to confirm they both have unit root i.e. both are non-stationary.
#                     # Perform ADF test for asset A
#                     adf_result_A = adfuller(log_price1)
#                     adf_statistic_A = adf_result_A[0]
#                     p_value_A = adf_result_A[1]
                    
#                     # Perform ADF test for asset B
#                     adf_result_B = adfuller(log_price2)
#                     adf_statistic_B = adf_result_B[0]
#                     p_value_B = adf_result_B[1]
                    
#                     #If both series are non stationary:
#                     if p_value_A > significance and p_value_B > significance:
                
#                         #Permorming engle_granger test.
#                         coint_result = engle_granger(log_price.iloc[:, 0], log_price.iloc[:, 1], trend="c", lags=0)
#                         pvalue = coint_result.pvalue
                        
#                         if pvalue < significance: ##reject null hypo, which implied series is not stationary.
#                             cointegrated_pair_list.append([asset1, asset2])
                        
#                         file.write(f"{asset1} - {asset2}: p value = {pvalue}\n")
                    
#         # Write pairs that pass the test to the file
#         file.write("Pairs passing the test:\n")
#         for pair in cointegrated_pair_list:
#             file.write(f"{pair[0]} - {pair[1]}\n")


#Pairs trading for AKBNK-YKBNK

myCSV = pd.DataFrame(columns = [ "Date_open", 'Date_close','Side','Hedge_Ratio','Multiplier','AKBNK_price_open',
                                 'AKBNK_price_close','YKBNK_price_open','YKBNK_price_close', 'Return (%)' ])

time_interval = 248
significance = 0.05
start_date = "2022-11-01"
end_date = "2023-06-30"

price_AKBNK = yf.download("AKBNK.IS", start=start_date, end=end_date,interval='1h', repair=True)
price_AKBNK = price_AKBNK.dropna()  # drop rows with nan values
price_AKBNK = price_AKBNK.drop(columns = ['Open', 'High','Low','Adj Close','Volume','Repaired?'])

price_YKBNK = yf.download("ISCTR.IS", start=start_date, end=end_date,interval='1h', repair=True)
price_YKBNK = price_YKBNK.dropna()  # drop rows with nan values
price_YKBNK = price_YKBNK.drop(columns = ['Open', 'High','Low','Adj Close','Volume','Repaired?'])


extra_date = price_AKBNK.index.difference(price_YKBNK.index)
price_AKBNK = price_AKBNK.drop(extra_date)


log_price1 = np.log(price_AKBNK)
log_price2 = np.log(price_YKBNK)

log_price = log_price1.join(log_price2, lsuffix='_1', rsuffix='_2')


#ADF on both assets to confirm they both have unit root i.e. both are non-stationary.
# Perform ADF test for asset A
adf_result_A = adfuller(log_price1)
adf_statistic_A = adf_result_A[0]
p_value_A = adf_result_A[1]

# Perform ADF test for asset B
adf_result_B = adfuller(log_price2)
adf_statistic_B = adf_result_B[0]
p_value_B = adf_result_B[1]

#If both series are non stationary:
if '''p_value_A > significance and p_value_B > significance''':

    #Permorming engle_granger test.
    coint_result = engle_granger(log_price.iloc[:, 0], log_price.iloc[:, 1], trend="c", lags=0)
    pvalue = coint_result.pvalue
    coint_vector = coint_result.cointegrating_vector
    
    hedge_ratio = -coint_vector[1]
    constant = coint_vector[2]
    
    spread_all = log_price.iloc[:, 0] - hedge_ratio * log_price.iloc[:, 1] + constant
    
    #☺For every 1 AKBNK, long/short hedge_ratio amount of YKBNK.
    plt.figure(figsize=(10, 6))
    plt.plot(spread_all)
    plt.xlabel("Time")
    plt.ylabel("Spread")
    plt.title("Spread Graph")
    plt.grid(True)
    plt.show()
    
    #Parameter Tuning I guess?
    entry_list = np.arange(0.5, 1.8, 0.1) #13 different values
    exit_list = np.arange(0, 0.55, 0.05) #11 different values
    stop_loss = [3,100] #2 different values
    #A total of 286 different combinations.
    
    #For Testing
    entry_list = [1.3]
    exit_list = [0.15]
    stop_loss = [100]
    
    # Perform pair trading
    for entry_threshold in entry_list:
        for exit_threshold in exit_list:
            for stop_loss_threshold in stop_loss:
    
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
            
            
                output_dir = "P:\Can Ceylan\Pair Trading\Test_multiplier"
                filename = os.path.join(output_dir, f'AKBNK_ISCTR_{Total_Return:.2f}_{entry_threshold:.2f}-{exit_threshold:.2f}-{stop_loss_threshold:.2f}.csv')
                myCSV.to_csv(filename, index=False)










# # Transform the return data using the fitted PCA model
# principal_components = pca.transform(return_list)

# # Calculate the correlation matrix between the return data
# correlation_matrix = np.corrcoef(principal_components, rowvar=False)

# correlation_threshold = 0.8
# num_assets = correlation_matrix.shape[0]
# potential_pairs = []

# for i in range(num_assets):
#     for j in range(i + 1, num_assets):
#         correlation = correlation_matrix[i, j]
#         if correlation > correlation_threshold:
#             pair = (tickerStrings[i], tickerStrings[j])
#             potential_pairs.append(pair)



# for pair in potential_pairs:
#     asset1 = pair[0]
#     asset2 = pair[1]
    
#     # Get the price data for the pair
#     price1 = df_list[tickerStrings.index(asset1)]['Adj Close']
#     price2 = df_list[tickerStrings.index(asset2)]['Adj Close']
    
#     # Plot the price data for the pair
#     fig, ax = plt.subplots()
#     ax.plot(price1.index, price1, label=asset1)
#     ax.plot(price2.index, price2, label=asset2)
    
#     ax.set_xlabel('Date')
#     ax.set_ylabel('Price')
#     ax.set_title(f'Time/Price Graph: {asset1} vs {asset2}')
#     ax.legend()
#     plt.show()


