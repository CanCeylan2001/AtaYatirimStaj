# Author: Can Ceylan
# Python GUI application to see the statistical changes of certain assets with the given parameters, such as
# starting and ending dates, return types, etc.

# If you want to channge the default settings of some parameters, change it from both the global variables below,
# and the elements at the bottom of the code.

import yfinance as yf # To fetch data.
import pandas as pd # Pandas dataframes are utilized.
import tkinter as tk #  Basically how we create GUI objects.
from tkinter import ttk, messagebox # For creating error boxes when input is not in expected format.
import datetime # To get the current date, to assign default values of the date parameters.
from tkcalendar import DateEntry # To achieve the calendar-like date entry object.
from dateutil.relativedelta import relativedelta # Used to calculate the defualt start date from the default end date.

# Where the data is stored.
filename = "D:\Ata Yatırım Staj\Can Ceylan\Heat Map Interface\data.parquet"
# Read data.
myDataFrame = pd.read_parquet(filename)

# Categories of stocks.
BIST30 = ["AKBNK.IS", 'ALARK.IS', 'ARCLK.IS','ASELS.IS','BIMAS.IS', 'EKGYO.IS','ENKAI.IS','EREGL.IS','FROTO.IS','GARAN.IS',
                  'GUBRF.IS', 'HEKTS.IS', 'ISCTR.IS', 'KCHOL.IS', 'KOZAA.IS', 'KOZAL.IS', 'KRDMD.IS', 'ODAS.IS', 'PETKM.IS', 'PGSUS.IS',
                  'SAHOL.IS', 'SASA.IS', 'SISE.IS', 'TAVHL.IS', 'TCELL.IS', 'THYAO.IS', 'TOASO.IS', 'TUPRS.IS', 'YKBNK.IS']

BIST50 = ['THYAO.IS', 'TUPRS.IS', 'BIMAS.IS', 'SISE.IS', 'SASA.IS', 'AKBNK.IS', 'KCHOL.IS', 'EREGL.IS', 'TCELL.IS',
                  'FROTO.IS', 'SAHOL.IS', 'ISCTR.IS', 'YKBNK.IS', 'ASELS.IS', 'PGSUS.IS', 'TOASO.IS', 'HEKTS.IS', 'MGROS.IS',
                  'GARAN.IS', 'KOZAL.IS', 'GUBRF.IS', 'ENKAI.IS', 'PETKM.IS', 'OYAKC.IS', 'KONTR.IS', 'TAVHL.IS',
                  'AEFES.IS', 'DOAS.IS', 'EKGYO.IS', 'KRDMD.IS', 'ARCLK.IS', 'SOKM.IS', 'SMRTG.IS', 'ALARK.IS', 'ENJSA.IS',
                  'CIMSA.IS', 'DOHOL.IS', 'ODAS.IS', 'AKSA.IS', 'GESAN.IS', 'AKSEN.IS', 'KOZAA.IS', 'TTKOM.IS', 'HALKB.IS',
                  'VESTL.IS', 'TKFEN.IS', 'VAKBN.IS', 'EGEEN.IS', 'TSKB.IS']

BIST100 = ['AEFES.IS', 'ULKER.IS', 'AKBNK.IS', 'AKCNS.IS', 'AKSA.IS', 'ALARK.IS', 'ARCLK.IS', 'ASELS.IS', 'ASUZU.IS', 'BAGFS.IS',
                  'BRSAN.IS', 'BRYAT.IS', 'BUCIM.IS', 'CEMTS.IS', 'CIMSA.IS', 'DOHOL.IS', 'ECILC.IS', 'ECZYT.IS', 'EGEEN.IS', 'ENKAI.IS',
                  'EREGL.IS', 'FROTO.IS', 'GARAN.IS', 'GLYHO.IS', 'GSDHO.IS', 'GUBRF.IS', 'HEKTS.IS', 'IPEKE.IS', 'ISCTR.IS', 'ISGYO.IS',
                  'IZMDC.IS', 'KARSN.IS', 'KCHOL.IS', 'KONYA.IS', 'KORDS.IS', 'KRDMD.IS', 'MGROS.IS', 'OYAKC.IS', 'OTKAR.IS', 'PETKM.IS',
                  'SAHOL.IS', 'SASA.IS', 'SISE.IS', 'SKBNK.IS', 'TCELL.IS', 'THYAO.IS', 'TOASO.IS', 'TSKB.IS', 'TUKAS.IS', 'TUPRS.IS', 'VESTL.IS',
                  'AGHOL.IS', 'YKBNK.IS', 'ZOREN.IS', 'KOZAA.IS', 'TTRAK.IS', 'DOAS.IS', 'BIMAS.IS', 'VAKBN.IS', 'VESBE.IS', 'SELEC.IS',
                  'CCOLA.IS', 'TAVHL.IS', 'HALKB.IS', 'ISMEN.IS', 'SNGYO.IS', 'ALBRK.IS', 'TKFEN.IS', 'TTKOM.IS', 'KOZAL.IS', 'AKSEN.IS',
                  'EKGYO.IS', 'AKFGY.IS', 'TKNSA.IS', 'BERA.IS', 'PGSUS.IS', 'ODAS.IS', 'ISDMR.IS', 'MAVI.IS', 'ENJSA.IS', 'SOKM.IS',
                  'KONTR.IS', 'QUAGR.IS', 'AYDEM.IS', 'GWIND.IS', 'CANTE.IS', 'PENTA.IS', 'KZBGY.IS', 'GESAN.IS',
                  'KMPUR.IS', 'SMRTG.IS', 'YYLGD.IS', 'GENIL.IS', 'AHGAZ.IS']

ATA = ['ATAGY.IS', 'ATATP.IS', 'ATAKP.IS']

# Storing categories in dictionary.
ticker_dict = {'BIST30' : BIST30,
               'BIST50' : BIST50,
               'BIST100' : BIST100,
               'ATA' : ATA}
# Function to check if the stored data is up to date, if not; fetch missing data from yahoo finance and update the stored file.
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
        myDataFrame.to_parquet(filename, index=True)


update_data() # Update myDataFrame with yahoo finance API.
selected_settings=["Simple Return"] # Which statistics to display, only works for C-C return.
current_display = '' # The current display, used to check if display needs to be updated.
selected_option = 'BIST30' # Which category of stocks to be displayed.
selected_option2 = 'Simple Return' # Which sorting methods needs to be applied. Only works for C-C return; defaults to Simple Return for other return types.
grid_frames = [[]] # Grid object where the frames are drawn.
selected_filter = "None" # Selected filter to apply.

valid_chars = {'O', 'C', 'H', 'L'}
valid_combinations = [] # Stores the valid return types, used for input checks.
for ch1 in valid_chars:
    for ch2 in valid_chars:
        valid_combinations.append(ch1 + ch2)

# The function to retrieve and aggregate tha data.
def get_data_by_date_range(start_date, end_date, myCol, T):
    global selected_filter
    data = 0 # To make data object have larger scope, may be redundant; excuse my python knowledge please.
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    new_rows = []  # List to store the new rows for concatenation
    if myCol != 'C-C':
        data = pd.DataFrame(columns = ['ticker', 'Simple Return'])
        
        if selected_filter == 'Moving Average':
            extra_weeks = int(T)//5 + 1
            new_start = start_date - pd.Timedelta(weeks=extra_weeks)
            
            first_valid = myDataFrame.head(1).index[0][1]
            if first_valid > new_start:
                raise Exception("Filter parameter is too high for the given date.")
            
            for ticker in ticker_dict[selected_option]:
                # Get the DataFrame for the current ticker
                ticker_data = myDataFrame.loc[ticker]
                
                ticker_data = ticker_data.loc[(ticker_data.index >= (new_start)) & (ticker_data.index <= end_date)]
                ticker_data['MA'] = ticker_data['Close'].rolling(window=int(T)).mean()
                ticker_data['MA<Close'] = ticker_data['MA'] < ticker_data['Close']
                ticker_data['filtered_return'] = ticker_data['MA<Close'].astype(int).shift(1) * ticker_data[myCol]
        
                selected_rows = ticker_data.loc[(ticker_data.index >= (start_date)) & (ticker_data.index <= end_date)]
                
                sum_column = selected_rows['filtered_return'].sum()
                new_row = {
                    'ticker' : ticker,
                    'Simple Return' : sum_column
                    }
                new_rows.append(new_row)
                
                
        elif selected_filter == 'None':
            
            for ticker in ticker_dict[selected_option]:
                # Get the DataFrame for the current ticker
                ticker_data = myDataFrame.loc[ticker]
        
                # Filter rows based on the dates within the specified range (inclusive)
                selected_rows = ticker_data.loc[(ticker_data.index >= start_date) & (ticker_data.index <= end_date)]
                
                sum_column = selected_rows[myCol].sum()
                new_row = {
                    'ticker' : ticker,
                    'Simple Return' : sum_column
                    }
                new_rows.append(new_row)
        
    else:
        
        data = pd.DataFrame(columns = ['ticker', 'Simple Return','Compound Return','Volume','Volatility','Positive Ratio','Kurtosis','Skew'])
        
        if selected_filter == 'Moving Average':
            extra_weeks = int(T)//5 + 1
            new_start = start_date - pd.Timedelta(weeks=extra_weeks)
            
            first_valid = myDataFrame.head(1).index[0][1]
            if first_valid > new_start:
                raise Exception("Filter parameter is too high for the given date.")
                
            for ticker in ticker_dict[selected_option]:
                # Get the DataFrame for the current ticker
                ticker_data = myDataFrame.loc[ticker]
                
                ticker_data = ticker_data.loc[(ticker_data.index >= (new_start)) & (ticker_data.index <= end_date)]
                ticker_data['MA'] = ticker_data['Close'].rolling(window=int(T)).mean()
                ticker_data['MA<Close'] = ticker_data['MA'] < ticker_data['Close']
        
                selected_rows = ticker_data.loc[(ticker_data.index >= (start_date)) & (ticker_data.index <= end_date)]
                selected_rows = selected_rows[selected_rows['MA<Close'].shift(1) == True]
                
                simple_return = selected_rows[myCol].sum()
                comp_return = ((((selected_rows[myCol]/100) + 1).prod()) - 1)* 100
                sum_volume = selected_rows['Volume'].sum()
                volatility = selected_rows[myCol].std() * (252**0.5)
                positive_ratio = ((selected_rows[myCol] > 0).sum() / len(selected_rows[myCol])) *100 if len(selected_rows[myCol]) > 0 else 0
                kurtosis = selected_rows[myCol].kurtosis()
                skew = selected_rows[myCol].skew()
                
                new_row = {
                    'ticker' : ticker,
                    'Simple Return' : simple_return,
                    'Compound Return' : comp_return,
                    'Total Volume' : sum_volume,
                    'Volatility' : volatility,
                    'Positive Ratio' : positive_ratio,
                    'Kurtosis' : kurtosis,
                    'Skew' : skew
                    }
                new_rows.append(new_row)
                
        elif selected_filter == 'None':
            for ticker in ticker_dict[selected_option]:
                # Get the DataFrame for the current ticker
                ticker_data = myDataFrame.loc[ticker]
        
                # Filter rows based on the dates within the specified range (inclusive)
                selected_rows = ticker_data.loc[(ticker_data.index >= start_date) & (ticker_data.index <= end_date)]
                
                simple_return = selected_rows[myCol].sum()
                comp_return = ((((selected_rows[myCol]/100) + 1).prod()) - 1)* 100
                sum_volume = selected_rows['Volume'].sum()
                volatility = selected_rows[myCol].std() * (252**0.5)
                positive_ratio = ((selected_rows[myCol] > 0).sum() / len(selected_rows[myCol])) *100 if len(selected_rows[myCol]) > 0 else 0
                kurtosis = selected_rows[myCol].kurtosis()
                skew = selected_rows[myCol].skew()
                
                new_row = {
                    'ticker' : ticker,
                    'Simple Return' : simple_return,
                    'Compound Return' : comp_return,
                    'Total Volume' : sum_volume,
                    'Volatility' : volatility,
                    'Positive Ratio' : positive_ratio,
                    'Kurtosis' : kurtosis,
                    'Skew' : skew
                    }
                new_rows.append(new_row)
        
        
    data = pd.concat([data, pd.DataFrame(new_rows)], ignore_index=True)
    data.set_index('ticker', inplace=True)
    
    sort_method = selected_option2 if myCol == 'C-C' else 'Simple Return'
    
    max_val = data[sort_method].max()  # Use the maximum value in the data list
    min_val = data[sort_method].min()  # Use the minimum value in the data list
    return data, max_val, min_val


def create_heatmap_grid():
    window = tk.Tk()
    window.title("Heatmap Grid")
    window.geometry("1050x750")

    
    # This function maps a numerical value to a color within a range, generating colors based on the value's magnitude.
    # It is used while generating a grid with the 'data', to assign colors to the frames.
    def value_to_color(value, max_val, min_val):
        # Map the numerical value to a color (dark green for large positive, light green for 0, and darker red for large negative)
    
        # Handle division by zero
        scale_factor_pos = 255 / max_val if max_val != 0 else 1
        scale_factor_neg = 255 / abs(min_val) if min_val != 0 else 1
    
        if value > 0:
            r = int(255 - value * scale_factor_pos)
            g = 255
            b = int(255 - value * scale_factor_pos)
        elif value < 0:
            r = 255
            g = int(255 - abs(value) * scale_factor_neg)
            b = int(255 - abs(value) * scale_factor_neg)
        else:
            r = 255
            g = 255
            b = 255
    
        return f'#{r:02x}{g:02x}{b:02x}'
    
    # Function that is executed with mousewheel event. If current display is not empty, it also calls the update_heatmap function.
    def on_mousewheel(event):
        global current_display
        
        if isinstance(event.widget, (ttk.Combobox, str)):
            return
    
        canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        if current_display != '':
            update_heatmap()
    

    def update_heatmap():
        global current_display, grid_frames, selected_filter, valid_combinations
        
        def update_visibility():
            if current_display != "":
                save_grid_button.place(x=600, y=40)  # Show the button
            else:
                save_grid_button.place_forget()  # Hide the button
                
        def save_current_grid(dataframe_to_be_saved,start_date,end_date,selected_option,selected_filter,T,myCol):
            filepath = f"D:/Ata Yatırım Staj/Can Ceylan/Heat Map Interface/saved_maps/{selected_option}_{myCol}_{start_date}-{end_date}"
            if selected_filter != "None":
                filepath += f"_{selected_filter}-{T}"
            filepath += ".csv"
            
            dataframe_to_be_saved.to_csv(filepath, index=True)
            
        
        start_date = start_date_entry.get()
        end_date = end_date_entry.get()
        
        canvas.update_idletasks()
        canvas.config(scrollregion=canvas.bbox("all"))
        ## Input checks before creating any grid. If input is wrong, appropriate message is displayed, and function returns.
        # Checking dates.
        if is_valid_date(start_date) == False or  is_valid_date(end_date)== False:
            messagebox.showerror("Invalid Date Format", "Please enter dates in the format YYYY-MM-DD")
            return
        
        char1 = char_entry.get().upper()
        #Check myRow, which is return type.
        if char1 not in valid_combinations:
            messagebox.showerror("Invalid Characters", "Please enter a combination of 'O', 'C', 'H', or 'L' characters.")
            return
        myCol = char1[0] + '-' + char1[1]
        # Checking category of stocks to be displayed.
        if selected_option not in ticker_dict:
            messagebox.showerror("Invalid Stock Option", "Please choose from the given options.")
            return
        # Checking Sorting Method, only works for C-C return type.
        if selected_option2 not in option_combobox2['values']:
            messagebox.showerror("Invalid Sorting Method", "Please choose from the given options.")
            return
        # Checking the selected filter.
        if selected_filter not in option_combobox3['values']:
            messagebox.showerror("Invalid Filter Method", "Please choose from the given options.")
            return
        T = filter_entry.get()
        # Cheking if the given parameter is valid according to the selected filter.
        if selected_filter == 'Moving Average' and T == "":
            messagebox.showerror("Invalid Filter Parameter", "Please fill the parameter box.")
            return
        
        
        rect_height = 100
        rect_width = 200
        
        myVal = 190 if myCol != 'C-C' else 300
        
        num_cols = window.winfo_width() // (myVal) + 1
        sort_method = selected_option2 if myCol=='C-C' else 'Simple Return'
        
        "Simple Return", "Compound Return", "Volatility", "Positive Ratio", "Total Volume", "Skew", "Kurtosis"
        
        # Created to later check if user tries to update the grid with the same parameters. If yes, the grid is not updated.
        def settings(selected_settings):
            myStr = ''
            if "Simple Return" in selected_settings:
                myStr += "Simple Return"
            if "Compound Return" in selected_settings:
                myStr += "Compound Return"
            if "Volatility" in selected_settings:
                myStr += "Volatility"
            if "Positive Ratio" in selected_settings:
                myStr += "Positive Ratio"
            if "Total Volume" in selected_settings:
                myStr += "Total Volume"
            if "Skew" in selected_settings:
                myStr += "Skew"
            if "Kurtosis" in selected_settings:
                myStr += "Kurtosis"
            return myStr
        
        displayed_settings = settings(selected_settings) if myCol == 'C-C' else 'Simple Return'
        # The current parameters used to create the grid. If it did not change, meaning user tries to update the grid
        # with the same parameters, the function returns.
        if current_display == start_date + end_date + myCol + selected_option + str(num_cols) + sort_method + displayed_settings + selected_filter + T:
            return
        # len(selected_settings) == 0 means user did not want to display any info about the assets. This does not make sense.
        if myCol == 'C-C' and len(selected_settings) == 0:
            messagebox.showerror("Invalid Display Setting","Please choose to display at least one setting.")
            return
        
        # The exception is raised when the parameter T given the selected filter causes a contradiction,
        # such as T value for moving average filter being too large, and not enough data is present.
        # Otherwise, data is filtered and stored is 'data' variable.
        # max_val = data.max()
        # min_val = data.min()
        try:
            data, max_val, min_val = get_data_by_date_range(start_date, end_date, myCol, T)
        except Exception as e:
            messagebox.showerror("Invalid Filter Parameter", e)
            return
        # calculating how many rows are needed.
        num_rows = (len(data) + num_cols - 1) // num_cols
        # If there was a grid, it needs to be destroyed before drawing a new grid.
        if len(grid_frames) != 0:
            for i in range(len(grid_frames)):
                for j in range(len(grid_frames[i])):
                    grid_frames[i][j].destroy()

        # Sort the data in descending order (from max to min), according to the sort method, which is 'Simple Return' for
        # return types other than 'C-C' by default. For C-C, it is equal to selected_option2
        sorted_data = data.sort_values(by=sort_method, ascending=False)
        
        #Creating the grid object.
        grid_frames = [[tk.Frame(grid_frame, width= rect_width, height= rect_height, borderwidth=1, relief=tk.GROOVE) for _ in range(num_cols)] for _ in range(num_rows)]

        # Setting the grid frames' places.
        for i in range(num_rows):
            for j in range(num_cols):
                grid_frames[i][j].grid(row=i, column=j, padx=5, pady=5)
                
        new_rows = []
        dataframe_to_be_saved = 0 # just so that is has larger scope.
        # If return type is not 'C-C', display is constructed simpler with default, predetermined ways.
        if myCol != 'C-C':

            for i in range(num_rows):
                for j in range(num_cols):
                    if i * num_cols + j < len(sorted_data):
                        myRow = sorted_data.iloc[i * num_cols + j]
                        asset = myRow.name[:-3]
                        value = myRow['Simple Return']
                        color = value_to_color(value, max_val, min_val)
                        label = tk.Label(grid_frames[i][j], text=f"{asset}\n{value:.2f}%", width=15, height=5, font=("Courier", 12), bg=color)
                        label.pack()
                        
                        new_row = {
                            'Asset' : asset,
                            'Simple Return' : value
                            }
                        new_rows.append(new_row)
                    else:
                        grid_frames[i][j].destroy()
            
            dataframe_to_be_saved = pd.DataFrame(new_rows)
        # Return type(myCol) is 'C-C'.        
        else:
            
            for i in range(num_rows):
                for j in range(num_cols):
                    if i * num_cols + j < len(sorted_data):
                        myRow = sorted_data.iloc[i * num_cols + j]
                        
                        asset = myRow.name[:-3]
                        asset_text = f"{asset}\n"
                        
                        value = myRow['Simple Return']
                        value_text = f"Simple Return: {value:.2f}%\n"
                        
                        volatility = myRow['Volatility']
                        volatility_text = f"Volatility: {volatility:.3f}\n"
                        
                        comp_return = myRow['Compound Return']
                        comp_return_text = f"Comp. Return: {comp_return:.2f}%\n"
                        
                        sum_volume = myRow['Total Volume']
                        sum_volume_text = f"Total Volume: {sum_volume:.1e}\n"
                        
                        skew = myRow['Skew']
                        skew_text = f"Skew: {skew:.2f}\n"
                        
                        kurtosis = myRow['Kurtosis']
                        kurtosis_text = f"Kurtosis: {kurtosis:.2f}"
                        
                        pos_ratio = myRow['Positive Ratio']
                        pos_ratio_text = f"Positive Ratio: {pos_ratio:.2f}%\n"
                        
                        myText = asset_text
                        
                        if "Simple Return" in selected_settings:
                            myText += value_text
                        if "Compound Return" in selected_settings:
                            myText += comp_return_text
                        if "Volatility" in selected_settings:
                            myText += volatility_text
                        if "Positive Ratio" in selected_settings:
                            myText += pos_ratio_text
                        if "Total Volume" in selected_settings:
                            myText += sum_volume_text
                        if "Skew" in selected_settings:
                            myText += skew_text
                        if "Kurtosis" in selected_settings:
                            myText += kurtosis_text
                        
                        color = value_to_color(myRow[sort_method], max_val, min_val)
                        label = tk.Label(grid_frames[i][j], text=myText, width=23, height=5+len(selected_settings), font=("Courier", 12), bg=color)
                        label.pack()
                        
                        new_row = {
                            'Asset' : asset,
                            'Simple Return' : value,
                            'Volatility' : volatility,
                            'Compound Return' : comp_return,
                            'Total Volume' : sum_volume,
                            'Skew' : skew,
                            'Kurtosis' : kurtosis,
                            'Positive Ratio' : pos_ratio
                            }
                        new_rows.append(new_row)
                        
                    else:
                        grid_frames[i][j].destroy()
            new_dataframe = pd.DataFrame(new_rows)
            selected_columns = ['Asset'] + selected_settings
            dataframe_to_be_saved = new_dataframe[selected_columns]
            
        # Updating current_display variable for the next iteration.
        current_display = start_date + end_date + myCol + selected_option + str(num_cols) + sort_method + displayed_settings + selected_filter + T
        dataframe_to_be_saved.set_index('Asset', inplace=True)
        #Button to save the current grid.
        save_grid_button = tk.Button(date_frame, text="Save Grid", command=lambda: save_current_grid(dataframe_to_be_saved,start_date,end_date,selected_option,selected_filter,T,myCol))
        save_grid_button.place(x=600,y=40)
        update_visibility()
        
    # Function to check if date is entered in the expected format.
    def is_valid_date(date_string):
        try:
            datetime.datetime.strptime(date_string, "%Y-%m-%d")
            return True
        except ValueError:
            return False

## 3 functions here are to assing the global variables its values, once an option from their respective combobox gets selected.
# selected_option: Selected index to display, such as BIST30, BIST50, ATA.
# selected_option2: Which value to sort the data by, such as Simple Return, Compound Return... Only works when return(myCol) is set to C-C.
# selected_option3: To select a filter to apply to the stocks, default is None. 
    
    def on_option_selected(event):
        global selected_option
        selected_option = option_combobox.get()
        
    def on_option_selected2(event):
        global selected_option2
        selected_option2 = option_combobox2.get()
        
    def on_option_selected3(event):
        global selected_filter
        selected_filter = option_combobox3.get()
        
    #The main frame where all the elements lie at.
    date_frame = tk.Frame(window)
    date_frame.pack(pady=10)    
    
    ## Generating Texts and user input fields...
    # Category of stocks.
    option_label = tk.Label(date_frame, text="Select an option:")
    option_label.grid(row=0, column=0, padx=5, pady=1)
    
    option_combobox = ttk.Combobox(date_frame, values=["BIST30", "BIST50", "BIST100", "ATA"])
    option_combobox.grid(row=1, column=0, padx=5, pady=1)
    option_combobox.bind("<<ComboboxSelected>>", on_option_selected)
    option_combobox.set("BIST30")
    
    # Sorting options only for CC return.
    option_label2 = tk.Label(date_frame, text="Sort by:")
    option_label2.grid(row=2, column=0, padx=5, pady=1)
    
    option_combobox2 = ttk.Combobox(date_frame, values=["Simple Return", "Compound Return", "Volatility", "Positive Ratio", "Total Volume", "Skew", "Kurtosis"])
    option_combobox2.grid(row=3, column=0, padx=5, pady=1)
    option_combobox2.bind("<<ComboboxSelected>>", on_option_selected2)
    option_combobox2.set("Simple Return")
    
    
    # For filters to be used.
    option_label3 = tk.Label(date_frame, text="Select filter:")
    option_label3.place(x=370,y=40)
    
    option_combobox3 = ttk.Combobox(date_frame, values=["None", "Moving Average"])
    option_combobox3.place(x=440,y=40)
    option_combobox3.bind("<<ComboboxSelected>>", on_option_selected3)
    option_combobox3.set("None")
    
    filter_label = tk.Label(date_frame, text="Enter parameter for filter:")
    filter_label.grid(row = 1, column = 1, padx = 10, pady=0)
    filter_label.place(x=370,y=70)

    filter_entry = tk.Entry(date_frame, width=4)
    filter_entry.grid(row = 1, column = 2, padx = 6, pady=0)
    filter_entry.place(x=510,y=70)

    
    # Date entry fields. Default dates are set below this comment.
    today = datetime.date.today() - pd.Timedelta(days=1)
    one_month_ago = today - relativedelta(months=1)
    
    start_date_label = tk.Label(date_frame, text="Start Date:")
    start_date_label.grid(row=0, column=1, padx=5, pady=0)
    
    start_date_entry = DateEntry(date_frame, width=16, background='darkblue', foreground='white', locale="tr_TR", borderwidth=2, date_pattern='yyyy-mm-dd', show_week_numbers=False)
    start_date_entry.grid(row=0, column=2, padx=5, pady=0)
    start_date_entry.set_date(one_month_ago.strftime('%Y-%m-%d'))
    
    end_date_label = tk.Label(date_frame, text="End Date:")
    end_date_label.grid(row=0, column=3, padx=5, pady=0)
    
    end_date_entry = DateEntry(date_frame, width=16, background='darkblue', foreground='white', locale="tr_TR", borderwidth=2, date_pattern='yyyy-mm-dd', show_week_numbers=False)
    end_date_entry.grid(row=0, column=4, padx=5, pady=0)
    end_date_entry.set_date(today.strftime('%Y-%m-%d'))


    # Button to update the grid. "Enter" button, and also scrolling with mouse also works the same way.
    update_button = tk.Button(date_frame, text="DISPLAY", command=update_heatmap)
    update_button.grid(row=0, column=5, padx=5, pady=0)
    
    # Return type entry.
    char_label = tk.Label(date_frame, text="Enter O-C-H-L Characters:")
    char_label.grid(row = 1, column = 1, padx = 10, pady=0)
    char_label.place(x=185,y=40)

    char_entry = tk.Entry(date_frame, width=3)
    char_entry.grid(row = 1, column = 2, padx = 6, pady=0)
    char_entry.place(x=335,y=40)
    char_entry.insert(0, 'CC')
    
    # Scrollbar.
    scrollbar_frame = tk.Frame(window)
    scrollbar_frame.pack(side=tk.RIGHT, fill=tk.Y)

    scrollbar = ttk.Scrollbar(scrollbar_frame, orient=tk.VERTICAL)
    scrollbar.pack(fill=tk.Y, side=tk.RIGHT)

    canvas = tk.Canvas(window, yscrollcommand=scrollbar.set)
    canvas.pack(fill=tk.BOTH, expand=True)

    scrollbar.config(command=canvas.yview)
    
    canvas.bind_all("<MouseWheel>", on_mousewheel)
    # Configure canvas scrolling region
    canvas.update_idletasks()
    canvas.config(scrollregion=canvas.bbox("all"))

    grid_frame = tk.Frame(canvas)
    canvas.create_window((0, 0), window=grid_frame, anchor="nw")
    
    # Works only for C-C return. User can select which values needs to be displayed from settings variable.
    def update_selected_settings(setting, var):
        global selected_settings
        if var.get() == 1:  # Checkbox is checked
            selected_settings.append(setting)
        else:  # Checkbox is unchecked
            if setting in selected_settings:
               selected_settings.remove(setting)
    
    settings = ["Simple Return", "Compound Return", "Volatility", "Positive Ratio", "Total Volume", "Skew", "Kurtosis"]
    for col, setting in enumerate(settings):
        var = tk.IntVar()
        if setting == "Simple Return":
            var.set(1)  # Check "Simple Return" by default
        checkbox = tk.Checkbutton(date_frame, text=setting, variable=var, command=lambda s=setting, v=var: update_selected_settings(s, v))
        checkbox.grid(row=4, column=col, sticky="w")
        

    canvas.update_idletasks()
    canvas.config(scrollregion=(0, 0, grid_frame.winfo_reqwidth(), grid_frame.winfo_reqheight()))
    window.bind("<Return>", lambda event: update_heatmap())


    window.mainloop()

if __name__ == "__main__":
    # Running the GUI application.
    create_heatmap_grid()

