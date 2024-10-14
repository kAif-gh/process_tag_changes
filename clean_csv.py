import csv

import pandas as pd


def drop_nan_rows(df, column_names):
    return df.dropna(subset=column_names)


def merge_csv_files(file1: str, file2: str, columns: list) -> pd.DataFrame:
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)
    df = pd.concat([df1, df2], ignore_index=True)
    df = df.drop_duplicates(subset=columns, keep='last')

    return df


"""
Cleans a CSV file by stripping leading and trailing spaces from OldTagName and NewTagName columns,
while keeping spaces in the middle intact.

Parameters:
input_file (str): Path to the input CSV file.
output_file (str): Path to the output CSV file where cleaned data will be saved.
"""


def strip_csv2(input_file, column_names, output_file):
    df = pd.read_csv(input_file)
    print("Formatted rows:")

    for column in column_names:
        # Find rows where the strip operation will make a change (i.e., they have leading/trailing spaces)
        modified_rows = df[df[column] != df[column].str.strip()]

        df[column] = df[column].str.strip()

        for index, row in modified_rows.iterrows():
            print(f"Row {index}: {row[column]} -> {df.at[index, column]}")  # Print before and after stripping
    df.to_csv(output_file, index=False)
    print(f"\nData cleaned and saved to {output_file}")

# csv_file = "./prod/right/tag_name_changes.csv"
# df = pd.read_csv(csv_file)

# csv_file_test = "strip_test.csv"
# strip_csv2(csv_file, ["OldTagName", "NewTagName"], "output_strip_test.csv")

# cleaned_df = drop_nan_rows(df, ["Dgraph Name"])
# cleaned_df = drop_nan_rows(df, ["createdTimeNewTag"])
# print(cleaned_df)

# cleaned_df = df.drop(columns=["createdTimeNewTag", "Dgraph Name", "New Name"])
# cleaned_df.to_csv("./prod/right/tep_id_changes.csv", index=False)

# merged_df = merge_csv_files('./RAW-data/tag_name_changes1Cleaned.csv', './RAW-data/tag_name_changes2Cleaned.csv',
# columns=["Dgraph Name"]) merged_df.to_csv('./RAW-data/tag_name_changes_final.csv', index=False)
