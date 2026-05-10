'''
Script to drop a column from the WeatherHistory.csv
Get weatherHistory.csv from the Data Folder
Drop Column: Loud Cover : Column: J
Create a new File call weatherHistoryNew.csv
Store inside Data Folder
'''

import pandas as pd

# Define paths for easy reference
input_file_path = '../Data/weatherHistory.csv'
output_file_path = '../Data/weatherHistoryNew.csv'
column_to_drop = 'Loud Cover'

# Step 1: Read the CSV file from the Data folder
df = pd.read_csv(input_file_path)

# Step 2: Drop the 'Loud Cover' column
df.drop(column_to_drop, axis=1, inplace=True)

# Step 3: Save the modified DataFrame to a new CSV file in the Data folder
df.to_csv(output_file_path, index=False)
