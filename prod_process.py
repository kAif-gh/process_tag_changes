import pandas as pd
from utils import count_datetime_occurrences, fetch_timestamps, fetch_and_update_tepid, fetch_metadata_and_update, store_timestamp_info_to_file

# GraphQL endpoint and query template
graphql_endpoint = "https://doggerbankprod.aurora.equinor.com/storm/meta"
scope_prod = "api://c8fd6e51-6dd5-415b-8d43-3bedb52aa75e"

csv_file = "./RAW-data/tag_name_changes2Cleaned.csv"
df = pd.read_csv(csv_file)

# Call the function to fetch timestamps and update tep ids based on multiple columns
column_mappings_time = [
    ('New Name', 'new tep_id')  
]
column_mappings = [
    ('Dgraph Name', 'old tep_id'),
    ('New Name', 'new tep_id')      
]

# Fetch timestamps for each column and collect results
timestamp_info = {}
for source_column, new_column in column_mappings_time:
    df_time, start_time, end_time, fetched_timestamps = fetch_timestamps(graphql_endpoint, scope_prod, df, source_column)
    timestamp_info[new_column] = {
        'start_time': start_time,
        'end_time': end_time,
        'fetched_timestamps': fetched_timestamps,
        'count': len(fetched_timestamps)
    }
df_time.to_csv("./prod/right/time_of_newTag_cleaned_final.csv", index=False)

for new_column, info in timestamp_info.items():
    print(f"Results for column '{new_column}':")
    print(f"Start Time (Oldest Timestamp): {info['start_time']}")
    print(f"End Time (Latest Timestamp): {info['end_time']}")
    print(f"All timestamps fetched: {info['fetched_timestamps']}, with number of {info['count']}")

store_timestamp_info_to_file(timestamp_info, './prod/right/timestamp_info_final_file.txt')

# Update DataFrame with tep ids
updated_df = fetch_and_update_tepid(graphql_endpoint, scope_prod, df, column_mappings)
updated_df.to_csv("./prod/right/tagname_tepid_final.csv", index=False)


# updated_df, timestamp_info, fetched_timestamps = fetch_metadata_and_update(df, graphql_endpoint, scope_prod, column_mappings, timestamp_column='New Name')

# updated_df.to_csv("./prod/tagname_tepid_and_time_final.csv", index=False)

# # Output results if timestamp info is available
# if timestamp_info:
#     print(f"Start Time (Oldest Timestamp): {timestamp_info['start_time']}")
#     print(f"End Time (Latest Timestamp): {timestamp_info['end_time']}")
#     print(f"All timestamps fetched: {timestamp_info['fetched_timestamps']}, with number of {timestamp_info['count']}")

unique_time = count_datetime_occurrences(fetched_timestamps)
print(f"unique timestamp for the final file Eniar: {unique_time}")

with open('./prod/right/timestamp_occurrences.txt', 'w') as file:
    for timestamp, count in unique_time.items():
        file.write(f"{timestamp}: {count}\n")

