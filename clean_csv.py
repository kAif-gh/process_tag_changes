import pandas as pd 

def drop_nan_rows(df, column_names):
    return df.dropna(subset=column_names)

def merge_csv_files(file1: str, file2: str, columns: list) -> pd.DataFrame:
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)
    df = pd.concat([df1, df2], ignore_index=True)
    df = df.drop_duplicates(subset=columns, keep='last')
    
    return df

def clean_csv_with_pandas(input_file, output_file):
    """
    Cleans a CSV file by stripping leading and trailing spaces from 'OldTagName' and 'NewTagName' columns,
    while keeping spaces in the middle intact.

    Parameters:
    input_file (str): Path to the input CSV file.
    output_file (str): Path to the output CSV file where cleaned data will be saved.
    """
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(input_file)

    # Apply the strip function to both 'OldTagName' and 'NewTagName' columns
    df['OldTagName'] = df['OldTagName'].str.strip()
    df['NewTagName'] = df['NewTagName'].str.strip()

    # Save the cleaned DataFrame back to a new CSV file
    df.to_csv(output_file, index=False)

    print(f"Data cleaned and saved to {output_file}")


def strip_csv(input_file, output_file):
    """
    Cleans a CSV file by stripping leading and trailing spaces from OldTagName and NewTagName columns,
    while keeping spaces in the middle intact.

    Parameters:
    input_file (str): Path to the input CSV file.
    output_file (str): Path to the output CSV file where cleaned data will be saved.
    """
    with open(input_file, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        cleaned_data = []
        
        # Read each row and strip spaces from the tag names
        for row in reader:
            cleaned_row = [element.strip() for element in row]  # Only removes leading and trailing spaces
            cleaned_data.append(cleaned_row)
    
    # Write cleaned data to the output file
    with open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(cleaned_data)

    print(f"Data cleaned and saved to {output_file}")

csv_file = "./prod/right/tagname_tepid_final.csv"

df = pd.read_csv(csv_file)

# cleaned_df = drop_nan_rows(df, ["Dgraph Name"])
# cleaned_df = drop_nan_rows(df, ["createdTimeNewTag"])
# print(cleaned_df)

cleaned_df = df.drop(columns=["createdTimeNewTag", "Dgraph Name", "New Name"])
cleaned_df.to_csv("./prod/right/tep_id_changes.csv", index=False)

# merged_df = merge_csv_files('./RAW-data/tag_name_changes1Cleaned.csv', './RAW-data/tag_name_changes2Cleaned.csv', columns=["Dgraph Name"])
# merged_df.to_csv('./RAW-data/tag_name_changes_final.csv', index=False)
