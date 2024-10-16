import requests
import time
import pandas as pd
from datetime import datetime
import subprocess
from collections import Counter

query_template = """
{
    queryScadaSignal(filter: { name: { eq: $tag_name } }) {
        tepId
        metadata {
            _provenanceRecordAuditRecordCreatedTimestamp
        }  
    }
}"""


def get_access_token(scope):
    """Fetch the Azure access token for the Dgraph API."""
    try:
        command = [
            "az", "account", "get-access-token",
            "--resource", "{}".format(scope),
            "--query", "accessToken",
            "--output", "tsv"
        ]
        access_token = subprocess.check_output(command).decode('utf-8').strip()
        # print(access_token)
        return access_token
    except subprocess.CalledProcessError as e:
        print(f"Error fetching access token: {e}")
        return None


def get_metadata_for_tag(graphql_endpoint, scope, tag_name, max_retries=5):
    """Query Dgraph for metadata of a given tag name using the access token, with retries."""
    query = query_template.replace("$tag_name", f'"{tag_name}"')

    # Initial attempt to get the access token
    access_token = get_access_token(scope)
    if not access_token:
        print("Failed to retrieve access token. Exiting function.")
        return []

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    attempts = 0
    while attempts < max_retries:
        try:
            response = requests.post(graphql_endpoint, json={'query': query}, headers=headers)

            # Check if the response is successful
            if response.status_code == 200:
                data = response.json()

                # print(data)

                scada_signals = data.get('data', {}).get('queryScadaSignal', [])
                return scada_signals
            else:
                print(f"Attempt {attempts + 1}/{max_retries} failed to fetch data for tag {tag_name}. "
                      f"Status Code: {response.status_code}, Response: {response.text}")

            # Increment attempt counter
            attempts += 1
            if attempts < max_retries:
                time.sleep(2)  # Optional: Add a delay between retries (e.g., 2 seconds)

        except Exception as e:
            print(f"Error querying Dgraph for tag {tag_name} on attempt {attempts + 1}: {e}")
            attempts += 1

            # Optional: Add a delay between retries
            if attempts < max_retries:
                time.sleep(2)  # Sleep for 2 seconds before the next attempt

    # After 5 failed attempts, regenerate the access token and try once more
    print(f"Max retries reached for tag {tag_name}. Attempting to regenerate the access token and retry.")
    access_token = get_access_token(scope)
    if not access_token:
        print("Failed to retrieve a new access token after max retries. Exiting function.")
        return []

    headers['Authorization'] = f'Bearer {access_token}'
    try:
        # Make one final attempt with the new access token
        response = requests.post(graphql_endpoint, json={'query': query}, headers=headers)
        if response.status_code == 200:
            data = response.json()
            scada_signals = data.get('data', {}).get('queryScadaSignal', [])
            return scada_signals
        else:
            print(f"Final attempt failed for tag {tag_name} after regenerating token. "
                  f"Status Code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print(f"Error querying Dgraph for tag {tag_name} after regenerating token: {e}")

    return []  # Return an empty list if all attempts fail


def fetch_timestamps(graphql_endpoint, scope, df, source_column):
    """
    Fetches timestamps for the specified column in the DataFrame.

    Parameters:
    - df (pd.DataFrame): The DataFrame to process.
    - source_column (str): The source column name to fetch timestamps from.

    Returns:
    - tuple: Start time, end time, and list of all fetched timestamps.
    """
    start_time = None
    end_time = None
    all_fetched_timestamps = []
    df['createdTimeNewTag'] = None
    for index, row in df.iterrows():
        tag = row[source_column]
        print(f"Querying for tag: {tag}")

        metadata_records = get_metadata_for_tag(graphql_endpoint, scope, tag)
        print(f"metadata_records: '{metadata_records}':")

        # If metadata records are None or not a list, skip this tag
        if metadata_records is None or not isinstance(metadata_records, list):
            print(f"No metadata records found for tag: {tag}")
            continue  # Skip to the next tag if no valid records

        for record in metadata_records:
            print(f"records: '{record}':")

            # Check if 'metadata' exists and is not None before accessing it
            if 'metadata' not in record or record['metadata'] is None:
                print(f"No metadata or invalid metadata found for record: {record}")
                continue

                # Check if the required timestamp field exists in the metadata
            if '_provenanceRecordAuditRecordCreatedTimestamp' not in record['metadata']:
                print(
                    f"Timestamp field '_provenanceRecordAuditRecordCreatedTimestamp' not found in metadata for record: {record}")
                continue

                # Fetch and process the timestamp field
            timestamp_str = record['metadata']['_provenanceRecordAuditRecordCreatedTimestamp']
            print(f"Fetched timestamp: {timestamp_str}")

            # Ensure proper formatting by slicing after the first dot for microseconds
            if '.' in timestamp_str:
                timestamp_str = timestamp_str[:timestamp_str.find('.') + 7] + 'Z'

            try:
                # Parse the fetched timestamp into a datetime object
                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ")

                df.at[index, 'createdTimeNewTag'] = timestamp

                all_fetched_timestamps.append(timestamp)

                if start_time is None:
                    start_time = timestamp
                if end_time is None:
                    end_time = timestamp

                if timestamp <= start_time:
                    start_time = timestamp

                if timestamp >= end_time:
                    end_time = timestamp

            except ValueError as ve:
                print(f"Error parsing timestamp {timestamp_str} for tag {tag}: {ve}")
                continue  # Skip to the next record if the timestamp is invalid

    return df, start_time, end_time, all_fetched_timestamps


def fetch_and_update_tepid(graphql_endpoint, scope, df, column_mappings):
    """
    Fetches tepId for specified columns in the DataFrame and updates new columns.

    Parameters:
    - df (pd.DataFrame): The DataFrame to process.
    - column_mappings (list of tuples): Each tuple contains (source_column_name, new_column_name).

    Returns:
    - pd.DataFrame: The updated DataFrame with new columns for tepId.
    """
    for source_column, new_column in column_mappings:
        df[new_column] = None

        for index, row in df.iterrows():
            tag = row[source_column]
            print(f"Querying for tag: {tag}")

            metadata_records = get_metadata_for_tag(graphql_endpoint, scope, tag)
            print(f"metadata_records: '{metadata_records}':")

            # If no metadata records are found or the response is not a list, continue to the next tag
            if metadata_records is None or not isinstance(metadata_records, list):
                print(f"No metadata records found for tag: {tag}. Setting {new_column} to None.")
                df.at[index, new_column] = None  # Set None explicitly for clarity
                continue

            # Check for tepId in metadata records and update the DataFrame
            tep_id_found = False
            for record in metadata_records:
                print(f"records: '{record}':")

                # If 'tepId' exists in the record, update the DataFrame
                if 'tepId' in record and record['tepId'] is not None:
                    tep_id = record['tepId']
                    print(f"Fetched tepId: {tep_id}")
                    df.at[index, new_column] = tep_id
                    tep_id_found = True
                    break  # Exit the loop as we found a valid tepId for this tag

            if not tep_id_found:
                print(f"No tepId found for tag: {tag}. Setting {new_column} to None.")
                df.at[index, new_column] = None

    return df


def fetch_metadata_and_update(df, graphql_endpoint, scope, column_mappings, timestamp_column=None):
    """
    Fetches metadata for specified columns in the DataFrame and updates new columns.
    Optionally fetches timestamps based on a designated column.

    Parameters:
    - df (pd.DataFrame): The DataFrame to process.
    - graphql_endpoint (str): The GraphQL endpoint for metadata queries.
    - scope (str): Authorization scope for API requests.
    - column_mappings (list of tuples): Each tuple contains (source_column_name, new_column_name) for tepId updates.
    - timestamp_column (str or None): Column name to fetch timestamps for (if any).

    Returns:
    - pd.DataFrame: The updated DataFrame with new columns for tepId and timestamps.
    - dict: Timestamp information if `timestamp_column` is provided; else, None.
    """
    timestamp_info = None
    all_fetched_timestamps = []
    start_time, end_time = None, None

    # Prepare columns for update
    for _, new_column in column_mappings:
        df[new_column] = None

    # If timestamp_column is specified, add a new column for the timestamp information
    if timestamp_column:
        df['createdTimeNewTag'] = None

    for index, row in df.iterrows():
        tag = row[timestamp_column]
        print(f"Querying for tag: {tag}")

        # Fetch metadata for the given tag
        metadata_records = get_metadata_for_tag(graphql_endpoint, scope, tag)
        print(f"metadata_records: '{metadata_records}'")

        # Skip if metadata is not a list or is empty
        if metadata_records is None or not isinstance(metadata_records, list):
            print(f"No metadata records found for tag: {tag}.")
            continue

        # Variables to store tepId and timestamp
        tep_id_found = False
        timestamp_found = False

        for record in metadata_records:
            print(f"Record: '{record}'")

            # Fetch and update tepId if column mappings are provided
            for source_column, new_column in column_mappings:
                if 'tepId' in record and record['tepId'] is not None and not tep_id_found:
                    df.at[index, new_column] = record['tepId']
                    tep_id_found = True
                    print(f"Updated '{new_column}' for tag: {tag} with tepId: {record['tepId']}")

            # Fetch and update timestamp if specified
            if timestamp_column and not timestamp_found:
                if 'metadata' in record and record['metadata'] and \
                        '_provenanceRecordAuditRecordCreatedTimestamp' in record['metadata']:
                    timestamp_str = record['metadata']['_provenanceRecordAuditRecordCreatedTimestamp']
                    print(f"Fetched timestamp: {timestamp_str}")

                    # Parse and clean up the timestamp
                    if '.' in timestamp_str:
                        timestamp_str = timestamp_str[:timestamp_str.find('.') + 7] + 'Z'

                    try:
                        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                        df.at[index, 'createdTimeNewTag'] = timestamp
                        all_fetched_timestamps.append(timestamp)

                        # Determine start and end time
                        if start_time is None or timestamp < start_time:
                            start_time = timestamp
                        if end_time is None or timestamp > end_time:
                            end_time = timestamp

                        timestamp_found = True
                        print(f"Updated 'createdTimeNewTag' for tag: {tag} with timestamp: {timestamp}")

                    except ValueError as ve:
                        print(f"Error parsing timestamp {timestamp_str} for tag {tag}: {ve}")

    # Collect timestamp info if timestamp_column is used
    if timestamp_column:
        timestamp_info = {
            'start_time': start_time,
            'end_time': end_time,
            'fetched_timestamps': all_fetched_timestamps,
            'count': len(all_fetched_timestamps)
        }

    return df, timestamp_info, all_fetched_timestamps


from collections import defaultdict
from datetime import datetime


def count_datetime_occurrences(timestamp_list):
    timestamp_count = defaultdict(int)

    for timestamp in timestamp_list:
        timestamp_count[timestamp] += 1

    return dict(timestamp_count)


def store_timestamp_info_to_file(timestamp_info, output_file='timestamp_info.txt'):
    """
    Stores the timestamp information in a text file.

    Parameters:
    - timestamp_info (dict): Dictionary containing timestamp data for each new column.
    - output_file (str): The name of the file to save the information in. Default is 'timestamp_info.txt'.
    """
    with open(output_file, 'w') as file:
        for new_column, info in timestamp_info.items():
            file.write(f"Results for column '{new_column}':\n")
            file.write(f"Start Time (Oldest Timestamp): {info['start_time']}\n")
            file.write(f"End Time (Latest Timestamp): {info['end_time']}\n")
            file.write(f"All timestamps fetched: {info['fetched_timestamps']}\n")
            file.write(f"Number of timestamps: {info['count']}\n\n")
    print(f"Timestamp information saved to '{output_file}'")

def count_created_time_occurrences(csv_file_path):
    df = pd.read_csv(csv_file_path)
    created_time_column = df['createdTimeNewTag']
    time_counts = Counter(created_time_column)
    time_counts_dict = dict(time_counts)
    print(f"This is the different created timestamp for new tags: \n '{time_counts_dict}'")
