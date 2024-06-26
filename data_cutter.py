import pandas as pd

# Define the input and output file paths
input_file_path = 'data/events.csv'
output_file_path = 'data/events_cut.csv'

# Read the CSV file into a DataFrame
df = pd.read_csv(input_file_path)

# Remove the last 10,000 rows
df_trimmed = df[:-10000] if len(df) > 10000 else df.iloc[0:0]  # Ensure it doesn't error out if there are less than 10,000 rows

# Write the resulting DataFrame to a new CSV file
df_trimmed.to_csv(output_file_path, index=False)

print(f"Trimmed data written to {output_file_path}")
