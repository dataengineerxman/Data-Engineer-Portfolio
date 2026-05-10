'''
Script to filter by High Humidity
Get weatherHistoryNew.csv from the Data Folder
Filter Rows: With Humidity Higher than 0.85 or Higher: Column Humidity
Create a new File call weatherHistoryHumidity.csv
Store inside Data Folder
'''

import pandas as pd

# Define paths for easy reference
input_file_path = '../Data/weatherHistoryNew.csv'
output_file_path = '../Data/weatherHistoryHumidity.csv'

# Step 1: Read the CSV file from the Data folder
df = pd.read_csv(input_file_path)

# Display number of rows in the original and filtered DataFrames
print(f"Original DataFrame has {len(df)} rows.")

# Step 2: Filter by High Humidity
filtered_df= df[df['Humidity'] > 0.85]

# Step 3: Save the modified DataFrame to a new CSV file in the Data folder
filtered_df.to_csv(output_file_path, index=False)

# Display number of rows in the original and filtered DataFrames
print(f"Original DataFrame has {len(filtered_df)} rows.")
