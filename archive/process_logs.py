import pandas as pd
import glob
import os

def process_call_logs():
    # Define the path to the data directory
    data_dir = 'data'
    output_file = os.path.join(data_dir, 'combined_call_logs.csv')
    
    # Find all CSV files matching the pattern
    file_pattern = os.path.join(data_dir, 'CallLogLastWeek_*.csv')
    csv_files = glob.glob(file_pattern)
    
    if not csv_files:
        print("No files found matching the pattern.")
        return

    print(f"Found {len(csv_files)} files to process: {csv_files}")

    # Read and concatenate all files
    dfs = []
    for file in csv_files:
        try:
            df = pd.read_csv(file)
            dfs.append(df)
            print(f"Read {len(df)} rows from {file}")
        except Exception as e:
            print(f"Error reading {file}: {e}")

    if not dfs:
        print("No data loaded.")
        return

    combined_df = pd.concat(dfs, ignore_index=True)
    initial_count = len(combined_df)
    print(f"Total rows before deduplication: {initial_count}")

    # Remove duplicates
    combined_df.drop_duplicates(inplace=True)
    final_count = len(combined_df)
    print(f"Total rows after deduplication: {final_count}")
    print(f"Removed {initial_count - final_count} duplicate rows.")

    # Save to new CSV
    combined_df.to_csv(output_file, index=False)
    print(f"Saved combined data to {output_file}")

if __name__ == "__main__":
    process_call_logs()
