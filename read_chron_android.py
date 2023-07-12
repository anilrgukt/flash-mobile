import os
import random
import math
import argparse

import pandas as pd
import numpy as np

from datetime import datetime
import datetime as dt

parser = argparse.ArgumentParser()
parser.add_argument('--path_to_csv', type=str,
                    default='./test_android.csv', help='path to the raw logs from Chronicle dashboard')
parser.add_argument('--path_to_save', type=str,
                    default=None, help='the processed log is stored here if not specified it generates a name based on participant ID')
parser.add_argument('--date', type=str,
                    default=None, help='please input date in YYYY-MM-DD format to save the data for the specific date')

args = parser.parse_args()
                    

interaction_types = ['Move to Foreground', 'Move to Background']

fname = args.path_to_csv    
save_date = args.date

df = pd.read_csv(fname)
df = df[df['interaction_type'].isin(interaction_types)]
ppt_id = df.iloc[1]['participant_id']

if args.path_to_save is None:
    save_name = str(ppt_id) + '_chronicle_android.csv'
else:
    save_name = args.path_to_save

# Sort the DataFrame by "event_timestamp" column to ensure the rows are in chronological order
df.sort_values('event_timestamp', inplace=True)

# Reset the index of the DataFrame
df.reset_index(drop=True, inplace=True)

# Create new columns for start and stop timestamps
df['start_timestamp'] = pd.NaT
df['stop_timestamp'] = pd.NaT

# Iterate over the DataFrame rows until the second-to-last row
for index, row in df.iterrows():
    if index < len(df) - 1:
        next_row = df.iloc[index + 1]
        if row['interaction_type'] == 'Move to Foreground':
            # Check if the next row has the same app_package_name, application_label, and interaction_type as 'Move to Background'
            if next_row['app_package_name'] == row['app_package_name'] and \
                    next_row['application_label'] == row['application_label'] and \
                    next_row['interaction_type'] == 'Move to Background':
                
                #print(df.iloc[index:index+2])
                # Set the start and stop timestamps
                df.at[index, 'start_timestamp'] = pd.to_datetime(row['event_timestamp'])
                df.at[index, 'stop_timestamp'] = pd.to_datetime(next_row['event_timestamp'])

# Remove rows where either the start or stop timestamp is missing
df = df.dropna(subset=['start_timestamp', 'stop_timestamp'])
df['date'] = pd.to_datetime(df['start_timestamp'])
df['date'] = df['date'].dt.date

df['start_timestamp'] = pd.to_datetime(df['start_timestamp'])
df['start_timestamp'] = df['start_timestamp'].dt.strftime('%H:%M:%S')

df['stop_timestamp'] = pd.to_datetime(df['stop_timestamp'])
df['stop_timestamp'] = df['stop_timestamp'].dt.strftime('%H:%M:%S')

# Reset the index of the DataFrame
df.reset_index(drop=True, inplace=True)

cols = df.columns.tolist()
#cols = ['study_id', 'participant_id', 'date', 'start_timestamp', 'stop_timestamp', 'username', 'application_label', 'app_package_name', 'event_timestamp', 'interaction_type', 'event_type', 'timezone', 'uploaded_at']

filtered_cols = ['study_id', 'participant_id', 'date', 'start_timestamp', 'stop_timestamp', 'username', 'application_label', 'app_package_name', 'event_timestamp', 'timezone']
df = df[filtered_cols]

df.username = df.username.astype(str)
df.username = df.username.apply(lambda x : "None" if x=="NaT" else x)

# Print the updated DataFrame
#print(df)
#print(df.iloc[:10])

if save_date is not None:
    new_df = df[df['date']==pd.to_datetime(save_date).date()]
    file_name = './' + str(ppt_id) + '_' + save_date + '.csv'
    new_df.to_csv(file_name, index=False)
    print('results saved at: ', file_name)
else:
    df.to_csv(save_name, index=False)
    print('results saved at: ', save_name)
