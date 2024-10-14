
import os
import pandas as pd

# Paths
input_csv_path = "data/tepids_cleaned.csv"
batch_dir = "./batches/"
os.makedirs(batch_dir, exist_ok=True)

df = pd.read_csv(input_csv_path)
total_rows = len(df)
batch_files = []

for i in range(0, total_rows, 100):
    batch_df = df.iloc[i:i+100]
    batch_file = os.path.join(batch_dir, f"batch_{i // 100}.csv")
    batch_df.to_csv(batch_file, index=False)
    batch_files.append(batch_file)

print(batch_files)