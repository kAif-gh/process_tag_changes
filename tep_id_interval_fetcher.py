import pandas as pd
from utils import count_datetime_occurrences, fetch_timestamps, fetch_and_update_tepid


# GraphQL endpoint and query template
graphql_endpoint = "https://doggerbankdev.dev.aurora.equinor.com/storm/meta"

csv_file = "./data2/raw-cleaned.csv"
df = pd.read_csv(csv_file)

# Call the function to fetch timestamps and update tep ids based on multiple columns
column_mappings_time = [
    ('New Name', 'new tep_id')       # Source column and new column
]

column_mappings = [
    ('Dgraph Name', 'old tep_id'),
    ('New Name', 'new tep_id')       # Source column and new column
]
# Fetch timestamps for each column and collect results
timestamp_info = {}
for source_column, new_column in column_mappings_time:
    df_time, start_time, end_time, fetched_timestamps = fetch_timestamps(graphql_endpoint, df, source_column)
    timestamp_info[new_column] = {
        'start_time': start_time,
        'end_time': end_time,
        'fetched_timestamps': fetched_timestamps,
        'count': len(fetched_timestamps)
    }
df_time.to_csv("./data2/time_of_newTag_cleaned2.csv", index=False)

# Output results for each column
for new_column, info in timestamp_info.items():
    print(f"Results for column '{new_column}':")
    print(f"Start Time (Oldest Timestamp): {info['start_time']}")
    print(f"End Time (Latest Timestamp): {info['end_time']}")
    print(f"All timestamps fetched: {info['fetched_timestamps']}, with number of {info['count']}")

# Update DataFrame with tep ids
updated_df = fetch_and_update_tepid(graphql_endpoint, df, column_mappings)

# Save the updated DataFrame to a new CSV file
updated_df.to_csv("tagname_tepid2.csv", index=False)