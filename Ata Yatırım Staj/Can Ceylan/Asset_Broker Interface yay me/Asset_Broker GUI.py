import pandas as pd
import numpy as np
#import matplotlib.pyplot as plt
# import pyarrow.parquet as pq
import tkinter as tk
from tkinter import ttk, messagebox
import re
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg



filename = "D:/Ata Yatırım Staj/Can Ceylan/Asset_Broker Interface yay me/todays_parsed_data.parquet"
myDataFrame = pd.read_parquet(filename)


firm_list = myDataFrame.columns.tolist()
asset_list = myDataFrame.index.get_level_values('Asset').unique().tolist()

current_display = ''
selected_option1 = 'All'
selected_option2 = 'AKBNK'
grid_frames1 = [[]]
grid_frames2 = [[]]

save_start = None
save_end = None
save_dataframe = None


def get_data_by_date_range(start_date, end_date):
    global selected_option1, selected_option2
    
    new_rows1 = []  # List to store the new rows for concatenation
    new_rows2 = []
    
    data1 = pd.DataFrame(columns = ['Var', 'Percentage'])
    data2 = pd.DataFrame(columns = ['Var', 'Percentage'])
    total_volume = 0
    if selected_option1 == 'All': ## all firms, displays should have firm info and percentage
    
        filtered_df = myDataFrame.loc[(myDataFrame.index.get_level_values('Timestamp') >= start_date) &
                 (myDataFrame.index.get_level_values('Timestamp') <= end_date) &
                 (myDataFrame.index.get_level_values('Asset') == selected_option2)]
        
        
        for firm in firm_list:
            
            percentage = filtered_df[firm].sum()
            total_volume+= abs(percentage)
            new_row = {
                'Var' : firm,
                'Percentage' : percentage*100
                }
            if percentage < 0:
                new_rows2.append(new_row)
            elif percentage > 0:
                new_rows1.append(new_row)
        total_volume /= 2    
    else:
        if selected_option2 == 'All':
            
            filtered_df = myDataFrame.loc[(myDataFrame.index.get_level_values('Timestamp') >= start_date) &
                     (myDataFrame.index.get_level_values('Timestamp') <= end_date)]
            
            for asset in asset_list:
                
                percentage = filtered_df.loc[filtered_df.index.get_level_values('Asset') == asset][selected_option1].sum()
                total_volume += abs(percentage)
                
                new_row = {
                    'Var' : asset,
                    'Percentage' : percentage*100
                    }
                if percentage < 0:
                    new_rows2.append(new_row)
                elif percentage > 0:
                    new_rows1.append(new_row)
                
        else:
            return None,None,None,None
            
    
    data1 = pd.concat([data1, pd.DataFrame(new_rows1)], ignore_index=True)
    data1.set_index('Var', inplace=True)
    
    data2 = pd.concat([data2, pd.DataFrame(new_rows2)], ignore_index=True)
    data2.set_index('Var', inplace=True)
    
    data1['Percentage'] = data1['Percentage'] / total_volume
    data2['Percentage'] = data2['Percentage'] / total_volume
    
    max_val1 = data1['Percentage'].max()  # Use the maximum value in the data list
    min_val2 = data2['Percentage'].min()  # Use the minimum value in the data list
    
    return data1, max_val1, data2, min_val2



def create_heatmap_grid():
    window = tk.Tk()
    window.title("Heatmap Grid")
    window.geometry("1920x1080")
    
    def save_graph():
        global save_start, save_end, save_dataframe, selected_option1, selected_option2
        
        filepath = f"D:/Ata Yatırım Staj/Can Ceylan/Asset_Broker Interface yay me/saved_plots/{selected_option2}_{selected_option1}_{save_start}-{save_end}.csv"
        save_dataframe.to_csv(filepath, index=True)


    def value_to_color(value, divident, is_positive):
        if is_positive:
            # Positive data: Green to White spectrum
            normalized_value = value / divident  # Normalize to [0, 1]
            color = np.array([255, 255, 255]) - ((np.array([255, 0, 255]) ) * normalized_value)
        else:
            # Negative data: Red to White spectrum
            normalized_value = value / divident  # Normalize to [0, 1]
            color = np.array([255, 255, 255]) - ((np.array([0, 255, 255])) * normalized_value)
        
        color = np.round(color).astype(int)
        return f'#{int(color[0]):02x}{int(color[1]):02x}{int(color[2]):02x}'
    
    def on_mousewheel(event):
        global current_display
        
        if isinstance(event.widget, (ttk.Combobox, str)):
            return
    
        canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        if current_display != '':
            update_heatmap()
    
    def validate_and_parse_time(input_time):
        # Define a regular expression pattern to match 4-digit time format (e.g., 1215, 0853)
        time_pattern = r'^\d{4}$'
        
        if re.match(time_pattern, input_time):
            hour = int(input_time[:2])
            minute = int(input_time[2:])
            
            if hour >= 0 and hour <= 23 and minute >= 0 and minute <= 59:
                return hour, minute
            else:
                return None, None  # Invalid hour or minute
        else:
            return None, None  # Invalid time format

    def update_heatmap():
        global current_display, grid_frames1, grid_frames2, selected_option1, selected_option2
        global save_start, save_end, save_dataframe
        
        
        start_hour, start_minute = validate_and_parse_time(hour1_entry.get())
        end_hour, end_minute = validate_and_parse_time(hour2_entry.get())
        
        save_start = hour1_entry.get()
        save_end = hour2_entry.get()
        
        canvas.update_idletasks()
        canvas.config(scrollregion=canvas.bbox("all"))
        
        rect_height = 100
        rect_width = 200
        
        num_cols = 5

        # Check if both fields contain valid dates
        if start_hour is None or end_hour is None:
            messagebox.showerror("Invalid Time Format", "Correct format is 4 digits; such as 0955")
            return
        if selected_option1 not in  option_combobox1['values']:
            messagebox.showerror("Invalid Firm Option", "Please choose from the given options.")
            return
        if selected_option2 not in option_combobox2['values']:
            messagebox.showerror("Invalid Asset Option", "Please choose from the given options.")
            return
        if selected_option1 == 'All' and selected_option2 == 'All':
            messagebox.showerror("Invalid Option Combination", "Both Asset and Firm can not be \"All\".")
            return
        
        start_date = pd.Timedelta(hours=start_hour, minutes=start_minute)
        end_date = pd.Timedelta(hours=end_hour, minutes=end_minute)
        
        if current_display == str(start_date) + str(end_date) + selected_option1 + selected_option2:
            return
        if len(myDataFrame.loc[(myDataFrame.index.get_level_values('Timestamp') >= start_date) &
                 (myDataFrame.index.get_level_values('Timestamp') <= end_date)]) == 0:
            messagebox.showerror("Invalid Time Combination", "No data between the given time.")
            return
            

        data1, max_val1, data2, min_val2 = get_data_by_date_range(start_date, end_date)
        
   
        # Clear previous graph if it exists
        if hasattr(canvas, "mpl_canvas"):
            canvas.mpl_canvas.get_tk_widget().destroy()
            save_button.config(state=tk.DISABLED)
            
        if len(grid_frames1) != 0:
            for i in range(len(grid_frames1)):
                for j in range(len(grid_frames1[i])):
                    grid_frames1[i][j].destroy()
        
        if len(grid_frames2) != 0:
            for i in range(len(grid_frames2)):
                for j in range(len(grid_frames2[i])):
                    grid_frames2[i][j].destroy()
        
        # Check if both options are not 'All'
        if max_val1 == None:
            
            # Calculate the cumulative sum
            filtered_df = myDataFrame.loc[(myDataFrame.index.get_level_values('Timestamp') >= start_date) &
                                         (myDataFrame.index.get_level_values('Timestamp') <= end_date) &
                                         (myDataFrame.index.get_level_values('Asset') == selected_option2)]
    
            cumulative_sum = filtered_df[selected_option1].cumsum()

            
            # Convert Timedelta index to numeric values for plotting
            numeric_timestamps = filtered_df.index.get_level_values('Timestamp').total_seconds()
            
            # Calculate positions and labels for x-axis values
            num_displayed = min(30, len(numeric_timestamps))
            positions = np.linspace(0, len(numeric_timestamps) - 1, num_displayed, dtype=int)
            labels = [f"{int(timestamp // 3600):02}:{int((timestamp // 60) % 60):02}"
                      for timestamp in numeric_timestamps[positions]]
            
            # Create a Matplotlib figure
            plt.figure(figsize=(10, 6))
            plt.plot(numeric_timestamps, cumulative_sum, marker='o')
            plt.xlabel('Time (hour:minute)')
            plt.ylabel(f'Cumulative {selected_option1}')
            plt.title(f"Cumulative {selected_option1} Time Series for {selected_option2}")
            plt.xticks(numeric_timestamps[positions], labels, rotation=45)  # Rotate the x-axis labels by 45 degrees
            plt.tight_layout()
                
            # Embed the Matplotlib figure in a Tkinter window
            canvas.mpl_canvas = FigureCanvasTkAgg(plt.gcf(), master=canvas)
            canvas.mpl_canvas.draw()
            canvas.mpl_canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
            save_button.config(state=tk.NORMAL)
            
            dataframe = pd.DataFrame({selected_option1: filtered_df[selected_option1]})
            dataframe['Cumulative Sum'] = dataframe[selected_option1].cumsum()
            dataframe.index = pd.MultiIndex.from_tuples([(ts.total_seconds() // 3600 * 100 + ts.total_seconds() % 3600 // 60, asset) for ts, asset in dataframe.index], names=['Timestamp', 'Asset'])
            dataframe.index = dataframe.index.set_levels(dataframe.index.levels[0].astype(int).astype(str).str.zfill(4).str[:2] + ":" + dataframe.index.levels[0].astype(int).astype(str).str.zfill(4).str[2:], level=0)
            save_dataframe = dataframe.copy()
            
            
        else:
            
            num_rows1 = (len(data1) + num_cols - 1) // num_cols
            num_rows2 = (len(data2) + num_cols - 1) // num_cols
           
            # Sort the data in descending order (from max to min)
            sorted_data1 = data1.sort_values(by='Percentage', ascending=False)
            sorted_data2 = data2.sort_values(by='Percentage', ascending=True)
            
            grid_frames1 = [[tk.Frame(grid_frame, width= rect_width, height= rect_height, borderwidth=1, relief=tk.GROOVE) for _ in range(num_cols)] for _ in range(num_rows1)]
            grid_frames2 = [[tk.Frame(grid_frame, width= rect_width, height= rect_height, borderwidth=1, relief=tk.GROOVE) for _ in range(num_cols)] for _ in range(num_rows2)]

            for i in range(num_rows1):
                for j in range(num_cols):
                    grid_frames1[i][j].grid(row=i, column=j, padx=5, pady=5)
            
            
            
            for i in range(num_rows2):
                for j in range(num_cols, num_cols + num_cols):
                    grid_frames2[i][j-num_cols].grid(row=i, column=j, padx=5, pady=5)
                    
                    
            
            for i in range(num_rows1):
                for j in range(num_cols):
                    if i * num_cols + j < len(sorted_data1):
                        myRow = sorted_data1.iloc[i * num_cols + j]
                        
                        asset = myRow.name
                        asset_text = f"{asset}\n"
                        
                        value = myRow['Percentage']
                        value_text = f"{value:.4f}%"
                        
                        myText = asset_text + value_text
                        
                        color = value_to_color(value, max_val1, True)
                        label1 = tk.Label(grid_frames1[i][j], text=myText, width=16, height=5, font=("Courier", 12), bg=color)
                        label1.pack(fill=tk.BOTH, expand=True)
                    else:
                        grid_frames1[i][j].destroy()
                        
            for i in range(num_rows2):
                for j in range(num_cols, num_cols+num_cols):
                    if i * num_cols + j-num_cols < len(sorted_data2):
                        myRow = sorted_data2.iloc[i * num_cols + j-num_cols]
                        
                        asset = myRow.name
                        asset_text = f"{asset}\n"
                        
                        value = myRow['Percentage']
                        value_text = f"{value:.4f}%"
                        
                        myText = asset_text + value_text
                        
                        color = value_to_color(value, min_val2, False)
                        label2 = tk.Label(grid_frames2[i][j-num_cols], text=myText, width=16, height=5, font=("Courier", 12), bg=color)
                        label2.pack(fill=tk.BOTH, expand=True)
                    else:
                        grid_frames2[i][j-num_cols].destroy()
                
            
        current_display = str(start_date) + str(end_date) + selected_option1 + selected_option2

    
    def on_option_selected1(event):
        global selected_option1
        selected_option1 = option_combobox1.get()
        
    def on_option_selected2(event):
        global selected_option2
        selected_option2 = option_combobox2.get()

    date_frame = tk.Frame(window)
    date_frame.pack(pady=10)    
    
    
    combobox1_values = ['All'] + firm_list
    combobox2_values = ['All'] + asset_list
    
    firm_label = tk.Label(date_frame, text="Select Firm")
    firm_label.grid(row=0, column=6, padx=5, pady=0)
    
    option_combobox1 = ttk.Combobox(date_frame, values=combobox1_values)
    option_combobox1.grid(row=1, column=6, padx=5, pady=1)
    option_combobox1.bind("<<ComboboxSelected>>", on_option_selected1)
    option_combobox1.set("All")
    
    asset_label = tk.Label(date_frame, text="Select Asset")
    asset_label.grid(row=0, column=5, padx=5, pady=1)
    
    option_combobox2 = ttk.Combobox(date_frame, values=combobox2_values)
    option_combobox2.grid(row=1, column=5, padx=5, pady=1)
    option_combobox2.bind("<<ComboboxSelected>>", on_option_selected2)
    option_combobox2.set("AKBNK")


    asset_label = tk.Label(date_frame, text="From Hour:")
    asset_label.grid(row=0, column=0, padx=5, pady=1)
    
    asset_label = tk.Label(date_frame, text="To Hour:")
    asset_label.grid(row=0, column=2, padx=5, pady=1)
    

    hour1_entry = tk.Entry(date_frame, width=6)
    hour1_entry.grid(row = 0, column = 1, padx = 1, pady=0)
    hour1_entry.insert(0, '0000')
    
    hour2_entry = tk.Entry(date_frame, width=6)
    hour2_entry.grid(row = 0, column = 3, padx = 1, pady=0)
    hour2_entry.insert(0, '2359')


    update_button = tk.Button(date_frame, text="DISPLAY", command=update_heatmap)
    update_button.grid(row=0, column=7, padx=5, pady=0)
    
    save_button = tk.Button(date_frame, text="Save Graph", command=save_graph, state=tk.DISABLED)
    save_button.grid(row=0, column=8)
    

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
    
    canvas.update_idletasks()
    canvas.config(scrollregion=(0, 0, grid_frame.winfo_reqwidth(), grid_frame.winfo_reqheight()))
    window.bind("<Return>", lambda event: update_heatmap())


    window.mainloop()



if __name__ == "__main__":
    create_heatmap_grid()