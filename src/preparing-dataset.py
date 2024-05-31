
import pandas as pd
from datetime import datetime

chunk_size = 10**3
start_date = datetime(2023, 5, 1)
end_date = datetime(2024, 4, 30)
columns_to_read = ['Unique Key', 'Created Date', 'Closed Date',
                   'Agency', 'Complaint Type', 'Descriptor',
                   'Location Type', 'Incident Address', 'Street Name',
                   'Status', 'Location', 'Community Board', 'Borough',
                   'Open Data Channel Type', 'Vehicle Type', 'Taxi Company Borough']

filtered_chunks = []

for chunk in pd.read_csv('datasets/311-requests-from-2010.csv',
                         chunksize=chunk_size,
                         usecols=columns_to_read,
                         parse_dates=['Created Date', 'Closed Date'],
                         date_format='%m/%d/%Y %I:%M:%S %p'):
    filtered_chunk = chunk[(chunk['Created Date'] >= start_date) & (chunk['Created Date'] <= end_date)]
    filtered_chunks.append(filtered_chunk)

filtered_df = pd.concat(filtered_chunks)
filtered_df.to_csv('datasets/311-last-year-requests.csv', index=False)
