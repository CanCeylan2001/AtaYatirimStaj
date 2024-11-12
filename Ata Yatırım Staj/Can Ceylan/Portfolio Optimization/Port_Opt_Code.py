import yfinance as yf
import pandas as pd
import datetime
from pypfopt.efficient_frontier import EfficientFrontier
from pypfopt.efficient_frontier import EfficientCVaR
from pypfopt import risk_models
from pypfopt import expected_returns
from pypfopt import objective_functions

filename2 = "D:\Ata Yatırım Staj\Can Ceylan\Heat Map Interface\data.parquet"
myDataFrame = pd.read_parquet(filename2)

BIST30 = ["AKBNK.IS", 'ALARK.IS', 'ARCLK.IS','ASELS.IS','BIMAS.IS', 'EKGYO.IS','ENKAI.IS','EREGL.IS','FROTO.IS','GARAN.IS',
                  'GUBRF.IS', 'HEKTS.IS', 'ISCTR.IS', 'KCHOL.IS', 'KOZAA.IS', 'KOZAL.IS', 'KRDMD.IS', 'ODAS.IS', 'PETKM.IS', 'PGSUS.IS',
                  'SAHOL.IS', 'SASA.IS', 'SISE.IS', 'TAVHL.IS', 'TCELL.IS', 'THYAO.IS', 'TOASO.IS', 'TUPRS.IS', 'YKBNK.IS']

BIST50 = ['THYAO.IS', 'TUPRS.IS', 'BIMAS.IS', 'SISE.IS', 'SASA.IS', 'AKBNK.IS', 'KCHOL.IS', 'EREGL.IS', 'TCELL.IS',
                  'FROTO.IS', 'SAHOL.IS', 'ISCTR.IS', 'YKBNK.IS', 'ASELS.IS', 'PGSUS.IS', 'TOASO.IS', 'HEKTS.IS', 'MGROS.IS',
                  'GARAN.IS', 'KOZAL.IS', 'GUBRF.IS', 'ENKAI.IS', 'PETKM.IS', 'OYAKC.IS', 'KONTR.IS', 'TAVHL.IS',
                  'AEFES.IS', 'DOAS.IS', 'EKGYO.IS', 'KRDMD.IS', 'ARCLK.IS', 'SOKM.IS', 'SMRTG.IS', 'ALARK.IS', 'ENJSA.IS',
                  'CIMSA.IS', 'DOHOL.IS', 'ODAS.IS', 'AKSA.IS', 'GESAN.IS', 'AKSEN.IS', 'KOZAA.IS', 'TTKOM.IS', 'HALKB.IS',
                  'VESTL.IS', 'TKFEN.IS', 'VAKBN.IS', 'EGEEN.IS', 'TSKB.IS']

ticker_dict = {'BIST30' : BIST30,
               'BIST50' : BIST50}


def update_data():
    global myDataFrame
    last_date = myDataFrame.iloc[-1].name[1].date()
    current_date = datetime.datetime.now().date() - pd.Timedelta(days=1)
    
    if current_date.weekday() == 5:  # Saturday
        current_date -= datetime.timedelta(days=1)
    elif current_date.weekday() == 6:  # Sunday
        current_date -= datetime.timedelta(days=2)
    
    if (current_date > last_date):
        last_date -= datetime.timedelta(days=4)
        
        ticker_symbols = ['AEFES.IS', 'ULKER.IS', 'AKBNK.IS', 'AKCNS.IS', 'AKSA.IS', 'ALARK.IS', 'ARCLK.IS', 'ASELS.IS', 'ASUZU.IS', 'BAGFS.IS',
                          'BRSAN.IS', 'BRYAT.IS', 'BUCIM.IS', 'CEMTS.IS', 'CIMSA.IS', 'DOHOL.IS', 'ECILC.IS', 'ECZYT.IS', 'EGEEN.IS', 'ENKAI.IS',
                          'EREGL.IS', 'FROTO.IS', 'GARAN.IS', 'GLYHO.IS', 'GSDHO.IS', 'GUBRF.IS', 'HEKTS.IS', 'IPEKE.IS', 'ISCTR.IS', 'ISGYO.IS',
                          'IZMDC.IS', 'KARSN.IS', 'KCHOL.IS', 'KONYA.IS', 'KORDS.IS', 'KRDMD.IS', 'MGROS.IS', 'OYAKC.IS', 'OTKAR.IS', 'PETKM.IS',
                          'SAHOL.IS', 'SASA.IS', 'SISE.IS', 'SKBNK.IS', 'TCELL.IS', 'THYAO.IS', 'TOASO.IS', 'TSKB.IS', 'TUKAS.IS', 'TUPRS.IS', 'VESTL.IS',
                          'AGHOL.IS', 'YKBNK.IS', 'ZOREN.IS', 'KOZAA.IS', 'TTRAK.IS', 'DOAS.IS', 'BIMAS.IS', 'VAKBN.IS', 'VESBE.IS', 'SELEC.IS',
                          'CCOLA.IS', 'TAVHL.IS', 'HALKB.IS', 'ISMEN.IS', 'SNGYO.IS', 'ALBRK.IS', 'TKFEN.IS', 'TTKOM.IS', 'KOZAL.IS', 'AKSEN.IS',
                          'EKGYO.IS', 'AKFGY.IS', 'TKNSA.IS', 'BERA.IS', 'PGSUS.IS', 'ODAS.IS', 'ISDMR.IS', 'MAVI.IS', 'ENJSA.IS', 'SOKM.IS',
                          'KONTR.IS', 'QUAGR.IS', 'AYDEM.IS', 'GWIND.IS', 'CANTE.IS', 'PENTA.IS', 'KZBGY.IS', 'GESAN.IS',
                          'KMPUR.IS', 'SMRTG.IS', 'YYLGD.IS', 'ATAGY.IS', 'ATATP.IS', 'ATAKP.IS', 'GENIL.IS', 'AHGAZ.IS']
    
    
        variable_dict = {}
    
        ################################################################
    
        for ticker in ticker_symbols:
            
            # Fetch the data from Yahoo Finance
            data = yf.download(ticker, start=last_date, end=current_date+ pd.Timedelta(days=1))
    
            # Create a Pandas DataFrame from the fetched data
            df = pd.DataFrame(data)
            
            variable_dict[ticker] = df
            
        ################################################################
    
        #Main Loop for creating the table
        for ticker in ticker_symbols:
            df = variable_dict[ticker]
            df[ticker] = ticker
            
            df['O-O'] = (df['Open'] / df['Open'].shift(1) - 1)*100
            df['C-C'] = (df['Adj Close']/ df['Adj Close'].shift(1) - 1)*100
            df['H-H'] = (df['High']/ df['High'].shift(1) - 1)*100
            df['L-L'] = (df['Low']/ df['Low'].shift(1) - 1)*100
            df['O-C'] = (df['Close']/ df['Open'] - 1)*100
            df['O-H'] = (df['High']/ df['Open'] - 1)*100
            df['O-L'] = (df['Low']/ df['Open'] - 1)*100
            df['C-O'] = (df['Open']/ df['Close'].shift(1) - 1)*100
            df['C-H'] = (df['High']/ df['Close'] - 1)*100
            df['C-L'] = (df['Low']/ df['Close'] - 1)*100
            df['H-O'] = (df['Open']/ df['High'] - 1)*100
            df['H-C'] = (df['Close']/ df['High'] - 1)*100
            df['H-L'] = (df['Low']/ df['High'] - 1)*100
            df['L-O'] = (df['Open']/ df['Low'] - 1)*100
            df['L-C'] = (df['Close']/ df['Low'] - 1)*100
            df['L-H'] = (df['High']/ df['Low'] - 1)*100
            
            df.drop(columns = ['High', 'Low', 'Open'], inplace=True)
            df['Date'] = pd.to_datetime(df.index)
            df.set_index([ticker, 'Date'], inplace=True)
            
        newDataFrame = pd.concat(variable_dict.values(), names=['Symbol'])
        myDataFrame = myDataFrame.rename_axis(index=['ticker', 'Date'])
        newDataFrame = newDataFrame.rename_axis(index=['ticker', 'Date'])
        myDataFrame = myDataFrame.combine_first(newDataFrame)
        myDataFrame.to_parquet(filename2, index=True)

update_data()

###############################################################################################################



def max_return_over_n_days(start_date, end_date, index, max_weight):
    
   
    # start_date = pd.to_datetime(start_date)
    # end_date = pd.to_datetime(end_date)
    
    new_rows = []  # List to store the new rows for concatenation
   
    data = pd.DataFrame(columns = ['Ticker', 'Compound Return'])
    
    for ticker in ticker_dict[index]:
        # Get the DataFrame for the current ticker
        ticker_data = myDataFrame.loc[ticker]

        # Filter rows based on the dates within the specified range (inclusive)
        selected_rows = ticker_data.loc[(ticker_data.index >= start_date) & (ticker_data.index <= end_date)]
        
        sum_column = ((((selected_rows['C-C']/100) + 1).prod()) - 1)* 100
        new_row = {
            'Ticker' : ticker,
            'Compound Return' : sum_column
            }
        new_rows.append(new_row)
    data = pd.concat([data, pd.DataFrame(new_rows)], ignore_index=True)
        
    sorted_data = data.sort_values(by='Compound Return', ascending=False)
    myNum = 1 // max_weight
    current_weight = myNum * max_weight
    
    index_list = sorted_data.head(int(myNum+1)).index
    
    sorted_data.rename(columns={'Compound Return' : 'Weight'}, inplace=True)
    
    sorted_data['Weight'] = 0
    sorted_data.loc[index_list, 'Weight'] = max_weight
    
    if current_weight != 1:
        sorted_data.loc[index_list[-1], 'Weight'] = 1-current_weight
   
    sorted_data.set_index('Ticker', inplace=True)
    return sorted_data['Weight'].to_dict()


def clean_weights(weights, gamma):
    df = pd.DataFrame(list(weights.items()), columns=['Ticker', 'Weight'])
    df.loc[df['Weight'] < gamma, 'Weight'] = 0
    total_weight = df['Weight'].sum()
    df.loc[:,'Weight'] = df['Weight'] / total_weight
    return df.set_index('Ticker')['Weight'].to_dict()




def min_volatility_portfolio(start_date, end_date, index, max_weight,min_weight,gamma):
    
    
    # start_date = pd.to_datetime(start_date)
    # end_date = pd.to_datetime(end_date)
    
    data_pivot = myDataFrame.pivot_table(values="Adj Close", index="Date", columns="ticker")
    
    data = data_pivot.loc[(data_pivot.index >= start_date) & (data_pivot.index <= end_date)]
    
    data = data.loc[:, ticker_dict[index]]
    
    # Drop columns with NaN values
    data = data.dropna(axis=1)
    
    mu = expected_returns.mean_historical_return(data)  #compounding=False by default
    S = risk_models.CovarianceShrinkage(data).ledoit_wolf()
    
    ef = EfficientFrontier(mu, S, weight_bounds=(min_weight, max_weight))
    # ef.add_objective(objective_functions.L2_reg, gamma=gamma)
    # ef.add_constraint(lambda w: cp.sum(w) == 1)
    # ef.add_constraint(lambda w: cp.sum(w > 0) > min_stock_number-1)   just but 0.0000000000000001 many, this does not really make sense
    weights = ef.min_volatility()
    # cleaned_weights = ef.clean_weights()
    weights = clean_weights(weights, gamma)
    
    # ef.portfolio_performance(verbose=True)
    
    
    # return cleaned_weights
    return weights

def max_sharpe_portfolio(start_date, end_date, index, max_weight,min_weight,gamma):
    
    
    # start_date = pd.to_datetime(start_date)
    # end_date = pd.to_datetime(end_date)
    
    data_pivot = myDataFrame.pivot_table(values="Adj Close", index="Date", columns="ticker")
    
    data = data_pivot.loc[(data_pivot.index >= start_date) & (data_pivot.index <= end_date)]
    
    data = data.loc[:, ticker_dict[index]]
    
    # Drop columns with NaN values
    data = data.dropna(axis=1)
    
    mu = expected_returns.mean_historical_return(data)
    S = risk_models.CovarianceShrinkage(data).ledoit_wolf()
    
    # ef = EfficientFrontier(mu, S, weight_bounds=(min_weight, max_weight), solver="SCS")
    # ef.add_objective(objective_functions.L2_reg, gamma=1)
    # ef.add_constraint(lambda w: cp.sum(w) == 1)
    # ef.add_constraint(lambda w: cp.sum(w > 0) > min_stock_number-1)   just but 0.0000000000000001 many, this does not really make sense
    
    
    ef = EfficientFrontier(mu, S, weight_bounds=(min_weight,max_weight), risk_free_rate=0.15)
    weights = ef.nonconvex_objective(
            objective_functions.sharpe_ratio,
            objective_args=(ef.expected_returns, ef.cov_matrix),
            weights_sum_to_one=True)
    
    
    # weights = ef.nonconvex_objective(
    #     objective_functions.sharpe_ratio,
    #     objective_args=(ef.expected_returns, ef.cov_matrix),
    #     constraints=[
    #         {"type": "eq", "fun": lambda w: np.sum(w) - 1},  # sum to 1
    #         {"type": "ineq", "fun": lambda w: w - min_weight},  # greater than min_weight,
    #         {"type": "ineq", "fun": lambda w: max_weight - w},  # less than max_weight
    #     ],
    # )
    
    
    
    # weights = ef.max_sharpe(risk_free_rate=0)
    # # cleaned_weights = ef.clean_weights()
    weights = clean_weights(weights, gamma)
    # # print(f"gamma = 0.1, num nonzero = {sum(ef.weights > 0.001)}")
    # # ef.portfolio_performance(verbose=True)
    
    # # return cleaned_weights
    return weights


def min_dailyloss_portfolio(start_date, end_date, index, max_weight,min_weight,gamma):
    
    
    # start_date = pd.to_datetime(start_date)
    # end_date = pd.to_datetime(end_date)
    
    data_pivot = myDataFrame.pivot_table(values="Adj Close", index="Date", columns="ticker")
    
    data = data_pivot.loc[(data_pivot.index >= start_date) & (data_pivot.index <= end_date)]
    
    data = data.loc[:, ticker_dict[index]]
    
    # Drop columns with NaN values
    data = data.dropna(axis=1)
    
    mu = expected_returns.mean_historical_return(data)
    historical_returns = expected_returns.returns_from_prices(data)
    
    ef = EfficientCVaR(mu, historical_returns, weight_bounds=(min_weight, max_weight))
    # ef.add_objective(objective_functions.L2_reg, gamma=1)
    
    weights = ef.min_cvar()
    weights = clean_weights(weights, gamma)
    
    # ef.portfolio_performance(verbose=True)
    
    # return cleaned_weights
    return weights
    

def get_previous_monday(dt):
    # Calculate the difference in days between the current day and Monday (0).
    days_until_monday = (dt.weekday() - 0) % 7
    
    # Subtract the difference to get the previous Monday.
    previous_monday = dt - pd.Timedelta(days=days_until_monday)
    
    return previous_monday

if __name__ == "__main__" :
    
    myDict = {
        'Weekly, no constraint' : (1,7),
        'Weekly, max=0.25' : (0.25,7),
        'Monthly, no constraint' : (1,28),
        'Monthly, max=0.25' : (0.25,28)
        }
    
    file_path = 'D:\Ata Yatırım Staj\Can Ceylan/Portfolio Optimization/last_row.txt'
    with open(file_path, 'r') as file:
        last_timestamp = file.read()
        last_timestamp = pd.to_datetime(last_timestamp)
    
    last_monday = get_previous_monday(last_timestamp)
    
    END = pd.Timestamp(datetime.date.today())
    
    
    df_BIST30 = yf.download('XU030.IS', start=last_monday - pd.Timedelta(days=35), end=END)
    dfrep_BIST30 = yf.download('XU030.IS', start=last_monday - pd.Timedelta(days=35), end=END, repair=True)
    index_diff = dfrep_BIST30.index.difference(df_BIST30.index)
    df_BIST30 = dfrep_BIST30.drop(index_diff)
    
    for text, mytupple in myDict.items():
        
        end_date = last_monday - pd.Timedelta(days=mytupple[1])
        index = "BIST30"
        min_weight = 0
        max_weight = mytupple[0]
        gamma = 0.01
        
        dataframe_dict = {
            'Min Volatility' : pd.DataFrame(),
            'Min DailyLoss' : pd.DataFrame(),
            'Max Sharpe' : pd.DataFrame(),
            'Best 5 Return' : pd.DataFrame(),
            'Best 10 Return' : pd.DataFrame()
            }
        
        while end_date < END:
            
            start_date = end_date
            end_date += pd.Timedelta(days=7)
            end_date2 = start_date + pd.Timedelta(days=mytupple[1])
                     
            
            if end_date2 < END:
            
                min_volatility_basket = min_volatility_portfolio(start_date, end_date2, index, max_weight,min_weight,gamma)
                min_dailyloss_basket = min_dailyloss_portfolio(start_date, end_date2, index, max_weight,min_weight,gamma)
                max_sharpe_basket = max_sharpe_portfolio(start_date, end_date2, index, max_weight, min_weight,gamma)
                best_5_return_basket = max_return_over_n_days(start_date, end_date2, index, 0.2)
                best_10_return_basket = max_return_over_n_days(start_date, end_date2, index, 0.1)
                
                filename1 = f"D:\Ata Yatırım Staj\Can Ceylan/Portfolio Optimization/{text}/Assets/{start_date.date()}_{end_date2.date()}.txt"
                
                df1 = myDataFrame.xs('AKBNK.IS', level=0)
                filtered_asset1 = df1.loc[(df1.index >= start_date) & (df1.index <= end_date2)]
                last_day_oftheend_withprice = filtered_asset1.tail(1).index.item()
                last_day_oftheend_withprice = pd.to_datetime(last_day_oftheend_withprice)
                
                
                with open(filename1, "w") as f:
                    f.write("Min Volatility Portfolio:\n")
                    for asset, weight in min_volatility_basket.items():
                        if weight != 0:
                            f.write(f"{asset} : {weight}\n")
                    f.write("\nMin Daily Loss Portfolio:\n")
                    for asset, weight in min_dailyloss_basket.items():
                        if weight != 0:
                            f.write(f"{asset} : {weight}\n")
                    f.write("\nMax Sharpe Ratio Portfolio:\n")
                    for asset, weight in max_sharpe_basket.items():
                        if weight != 0:
                            f.write(f"{asset} : {weight}\n")
                    f.write("\nBest 5 Return Portfolio:\n")
                    for asset, weight in best_5_return_basket.items():
                        if weight != 0:
                            f.write(f"{asset} : {weight}\n")
                    f.write("\nBest 10 Return Portfolio:\n")
                    for asset, weight in best_10_return_basket.items():
                        if weight != 0:
                            f.write(f"{asset} : {weight}\n")
                
                variable_dict = {
                    'Min Volatility' : min_volatility_basket,
                    'Min DailyLoss' : min_dailyloss_basket,
                    'Max Sharpe' : max_sharpe_basket,
                    'Best 5 Return' : best_5_return_basket,
                    'Best 10 Return' : best_10_return_basket
                    }
                
                starting_money = 100000
                price_df_asset = {}
                for asset in ticker_dict['BIST30']:
                    
                    df = myDataFrame.xs(asset, level=0)
                    filtered_asset = df.loc[(df.index >= last_day_oftheend_withprice) & (df.index <= (last_day_oftheend_withprice + pd.Timedelta(days=7)))]['Adj Close']
                    filtered_asset = filtered_asset.to_frame()
                    filtered_asset.rename(columns = {'Adj Close' : asset}, inplace=True)
                    price_df_asset[asset] = filtered_asset
                    
                filtered_BIST30 = df_BIST30.loc[(df_BIST30.index >= last_day_oftheend_withprice) & (df_BIST30.index <= (last_day_oftheend_withprice + pd.Timedelta(days=7)))]['Adj Close']
                filtered_BIST30 = (filtered_BIST30 * (starting_money / filtered_BIST30.iloc[0])).to_frame()
                filtered_BIST30.rename(columns ={'Adj Close':'BIST30 Value'}, inplace=True)
                filtered_BIST30['BIST30 pct_change'] = filtered_BIST30['BIST30 Value'].pct_change()
                filtered_BIST30.dropna(inplace=True)
                
                for basket_type in variable_dict:
                    basket = variable_dict[basket_type]
                    collective_df = dataframe_dict[basket_type]
                    filename = f"D:\Ata Yatırım Staj\Can Ceylan/Portfolio Optimization/{text}/Seperate Results/{last_day_oftheend_withprice.date()}_{(last_day_oftheend_withprice + pd.Timedelta(days=7)).date()}_{basket_type}.csv"
                    stock_dict = {}
                    for asset, weight in basket.items():
                        filtered_asset = price_df_asset[asset]
                        asset_price = filtered_asset.iloc[0]
                        stock_dict[asset] = starting_money * weight / asset_price
                    
                    dataframe = pd.DataFrame()
                    
                    for asset in ticker_dict['BIST30']:
                        asset_amount = stock_dict[asset]
                        df_value_hist = price_df_asset[asset] * asset_amount
                        df_value_hist.columns = [asset]
                        dataframe = pd.concat([dataframe, df_value_hist], axis=1)
                    
                    dataframe['Portfolio Value'] = dataframe.sum(axis=1)
                    dataframe['Portfolio pct_change'] = dataframe['Portfolio Value'].pct_change()
                    dataframe.dropna(inplace=True)
                    
                    dataframe_to_csv = pd.concat([dataframe, filtered_BIST30], axis=1)
                    
                    columns_to_keep = ['Portfolio Value', 'BIST30 Value', 'Portfolio pct_change', 'BIST30 pct_change']
                    dataframe_to_csv = dataframe_to_csv[columns_to_keep]
                    dataframe_to_csv.to_csv(filename, index=True)
                    
                    columns_to_append = ['Portfolio pct_change', 'BIST30 pct_change']
                    df_to_append = dataframe_to_csv[columns_to_append]
                    collective_df = pd.concat([collective_df, df_to_append] , axis=0, join='outer')
                    dataframe_dict[basket_type] = collective_df
                    
                
                
                for basket_type, dataframe in dataframe_dict.items():
                     
                    filename3 = f"D:\Ata Yatırım Staj\Can Ceylan/Portfolio Optimization/{text}/Concatonated Results/{basket_type}.csv"
                    filtered_dataframe = dataframe[dataframe.index > last_timestamp]
                    filtered_dataframe.to_csv(filename3, mode='a', index=True, header=False)
                
    if len(dataframe_dict['Best 5 Return']) != 0:
        last_timestamp = dataframe_dict['Best 5 Return'].iloc[-1].name
        with open(file_path, 'w') as file:
            file.write(str(last_timestamp))
    
        
        
        
        
            
            
        
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    




















