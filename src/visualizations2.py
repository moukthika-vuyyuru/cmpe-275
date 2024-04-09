import pandas as pd
from multiprocessing import shared_memory, resource_tracker  
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.patches as mpatches

# Define the name of the shared memory segment
SHARED_MEM_NAME_BASELINE = "shared_memory_baseline"
SHARED_MEM_NAME = "county_data_shared_memory"

# Function to parse county data from shared memory into DataFrame
def parse_county_data_to_df(data):
    county_data = []

    current_county = None
    for line in data.split('\n'):
        line = line.strip()
        if line.startswith('County:'):
            current_county = line.split(': ')[1]
        elif line.startswith('Date:'):
            parts = line.split(', ')
            date = parts[0].split(': ')[1]
            max_aqi = int(parts[1].split(': ')[1])
            max_pollutant = parts[2].split(': ')[1]
            county_data.append([current_county, date, max_aqi, max_pollutant])

    return pd.DataFrame(county_data, columns=['County', 'Date', 'Max AQI', 'Max Pollutant'])

def parse_county_data(data):
    county_data = {}

    current_county = None
    for line in data.split('\n'):
        line = line.strip()
        if line.startswith('County:'):
            current_county = line.split(': ')[1]
            county_data[current_county] = {}
        elif line.startswith('Date:'):
            parts = line.split(', ')
            date = parts[0].split(': ')[1]
            max_aqi = int(parts[1].split(': ')[1])
            max_pollutant = parts[2].split(': ')[1]
            county_data[current_county][date] = {'Max AQI': max_aqi, 'Max Pollutant': max_pollutant}

    return county_data

# Function to read shared memory data into DataFrame
def read_shared_memory_to_df(shared_memory_name):
    shm_seg = shared_memory.SharedMemory(name=shared_memory_name)
    data = bytes(shm_seg.buf).strip(b'\x00').decode('ascii')
    shm_seg.close()

    resource_tracker.unregister(shm_seg._name, "shared_memory")

    return parse_county_data_to_df(data)

def calculate_max_aqi(baseline_df, county_df):
    # Calculate daily maximum AQI for each county during the baseline period (Aug 10th)
    baseline_max_aqi = baseline_df.groupby('County')['Max AQI'].max()

    # Calculate daily maximum AQI for each county during the period of California fires
    fires_max_aqi = county_df.groupby('County')['Max AQI'].max()

    return baseline_max_aqi, fires_max_aqi

def plot_aqi_comparison(baseline_max_aqi, fires_max_aqi):
    # Plotting
    plt.figure(figsize=(10, 6))

    # Plot bar chart for baseline max AQI
    plt.bar(baseline_max_aqi.index, baseline_max_aqi, color='blue', label='Baseline (Aug 10th)')

    # Plot bar chart for max AQI during fires
    plt.bar(fires_max_aqi.index, fires_max_aqi, color='red', alpha=0.5, label='During California Fires')

    # Set labels and title
    plt.xlabel('County')
    plt.ylabel('Max AQI')
    plt.title('Comparison of Max AQI during California Fires vs Baseline')
    plt.xticks(rotation=45)
    plt.legend()

    # Show plot
    plt.tight_layout()
    plt.show()

def plot_heatmap(county_df):
    plt.figure(figsize=(10, 8))
    heatmap = sns.heatmap(county_df.pivot_table(index='County', columns='Date', values='Max AQI'), cmap='Reds')
    plt.title('Spatial Distribution of AQI Levels')
    plt.xlabel('Date')
    plt.ylabel('County')
    # Rotate x-axis tick labels slantly
    heatmap.set_xticklabels(heatmap.get_xticklabels(), rotation=45, ha='right')
    
    plt.show()

def visualize_aqi_comparison_county_wise(baseline_df, county_df, target_county):
    # Strip white spaces from target county
    target_county_stripped = target_county.strip()

    # Filter data for the target county
    baseline_df_county = baseline_df[baseline_df['County'].str.strip() == target_county_stripped]
    county_df_county = county_df[county_df['County'].str.strip() == target_county_stripped]

    # Get unique dates in county_df_county
    unique_dates = county_df_county['Date'].unique()

    # Define colors for baseline and during fires
    baseline_color = 'blue'
    during_fires_color = 'red'

    # Plot baseline and AQI for the target county
    plt.figure(figsize=(10, 6))

    # Plot baseline bars for each date from Aug 14th to Sept 24th
    for date in unique_dates:
        baseline_max_aqi = baseline_df_county[baseline_df_county['Date'] == '20200810']['Max AQI'].values[0]
        plt.bar(date, baseline_max_aqi, color=baseline_color, alpha=0.5)

    # Plot AQI for each date from county_df_county with a different color
    for date in unique_dates:
        county_max_aqi = county_df_county[county_df_county['Date'] == date]['Max AQI'].values[0]
        plt.bar(date, county_max_aqi, color=during_fires_color, alpha=0.5)

    # Set labels and title
    plt.xlabel('Date')
    plt.ylabel('Max AQI')
    plt.title(f'Max AQI for {target_county_stripped} during California Fires vs Baseline')
    plt.xticks(rotation=45)
    
    # Create legend handles and labels
    baseline_patch = mpatches.Patch(color='purple', label='Baseline (Aug 10th)')
    during_fires_patch = mpatches.Patch(color=during_fires_color, label='During California Fires')
    
    # Add legend
    plt.legend(handles=[baseline_patch, during_fires_patch], loc='upper left')

    # Show plot
    plt.tight_layout()
    plt.show()


def visualize_aqi_comparison(shared_mem_baseline, shared_mem_county):
    baseline_df = read_shared_memory_to_df(shared_mem_baseline)
    county_df = read_shared_memory_to_df(shared_mem_county)

    baseline_max_aqi, fires_max_aqi = calculate_max_aqi(baseline_df, county_df)
    plot_aqi_comparison(baseline_max_aqi, fires_max_aqi)

    plot_heatmap(county_df)

    visualize_aqi_comparison_county_wise(baseline_df, county_df, 'SONOMA')

if __name__ == "__main__":
    visualize_aqi_comparison(SHARED_MEM_NAME_BASELINE, SHARED_MEM_NAME)
