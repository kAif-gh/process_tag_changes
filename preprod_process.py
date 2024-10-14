import pandas as pd
from utils import count_datetime_occurrences, fetch_timestamps, fetch_and_update_tepid

# GraphQL endpoint and query template
graphql_endpoint = "https://doggerbankpreprod.dev.aurora.equinor.com/storm/meta"
scope_dev_preprod = "api://8884d831-f8ef-41f3-b4d5-2d655d93b867"


csv_file = "./RAW-data/tag_name_changes_final.csv"
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
    df_time, start_time, end_time, fetched_timestamps = fetch_timestamps(graphql_endpoint, scope_dev_preprod, df, source_column)
    timestamp_info[new_column] = {
        'start_time': start_time,
        'end_time': end_time,
        'fetched_timestamps': fetched_timestamps,
        'count': len(fetched_timestamps)
    }
df_time.to_csv("./preprod/time_of_newTag_cleaned_final.csv", index=False)

# Output results for each column
for new_column, info in timestamp_info.items():
    print(f"Results for column '{new_column}':")
    print(f"Start Time (Oldest Timestamp): {info['start_time']}")
    print(f"End Time (Latest Timestamp): {info['end_time']}")
    print(f"All timestamps fetched: {info['fetched_timestamps']}, with number of {info['count']}")

# Update DataFrame with tep ids
updated_df = fetch_and_update_tepid(graphql_endpoint, scope_dev_preprod, df, column_mappings)

# Save the updated DataFrame to a new CSV file
updated_df.to_csv("./preprod/tagname_tepid_final.csv", index=False)

unique_time = count_datetime_occurrences(fetched_timestamps)
print(f"unique timestamp: {unique_time}")