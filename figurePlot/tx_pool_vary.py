import os
import pandas as pd
import matplotlib.pyplot as plt

# Define the directory where the CSV files are stored
directory = './expTest/result/pbft_shardNum=4'

# Get all CSV files in the directory
csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]

# Create an empty plot
plt.figure(figsize=(10, 6))

# Store file names (without the common suffix)
legend_labels = []

# Specify the position from which to start removing parts
x = 1

# Iterate over each CSV file and plot the line chart
for csv_file in csv_files:
    file_path = os.path.join(directory, csv_file)
    
    # Read data from the CSV file
    df = pd.read_csv(file_path)
    
    # Extract the required columns
    block_height = df['Block Height']
    tx_pool_size = df['TxPool Size']
    
    # Get the file name without the common suffix
    parts = csv_file.split('_')
    if len(parts) > x:
        file_name = '_'.join(parts[:-x])
    else:
        file_name = csv_file
    
    # Plot the line chart
    plt.plot(block_height, tx_pool_size, label=file_name)
    
    # Record the file name for the legend
    legend_labels.append(file_name)

# Add legend and labels
plt.xlabel('Block Height')
plt.ylabel('TxPool Size')
plt.title('TxPool Size vs Block Height')

# Set the legend
plt.legend()

# Show the plot
plt.grid(True)
plt.tight_layout()
plt.show()
