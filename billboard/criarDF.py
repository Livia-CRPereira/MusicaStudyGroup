import os
import pandas as pd

# Function to create a DataFrame from filenames in a folder
def create_dataframe(folder_path):
    filenames = [filename for filename in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, filename))]
    df = pd.DataFrame(filenames, columns=['filename'])
    return df

# Specify the folder path (assuming 'downloaded_audios' is in the same directory as the script)
folder_path = 'downloaded_audios'

# Create the DataFrame
df = create_dataframe(folder_path)

# Save the DataFrame to a CSV file
csv_filename = 'filenames.csv'
df.to_csv(csv_filename, index=False)

print(f"DataFrame created and saved to {csv_filename}.")
