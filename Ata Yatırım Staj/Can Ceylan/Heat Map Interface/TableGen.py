import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pyarrow.parquet as pq



# Set the start and end dates for the data
start_date = "2019-01-01"
end_date = "2023-08-16"


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
    data = yf.download(ticker, start=start_date, end=end_date)

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
    
myDataFrame = pd.concat(variable_dict.values(), names=['ticker', 'Date'])
myDataFrame = myDataFrame.rename_axis(index=['ticker', 'Date'])


path = "D:/Ata Yatırım Staj/Can Ceylan/Heat Map Interface/data.parquet"
myDataFrame.to_parquet(path, index=True)
    
    
    

