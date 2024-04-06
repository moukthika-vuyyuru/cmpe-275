import sys
from multiprocessing import shared_memory, resource_tracker  
import matplotlib.pyplot as plt 

# Define the name of the shared memory segment
SHARED_MEM_NAME_BASELINE = "shared_memory_baseline"
SHARED_MEM_NAME ="county_data_shared_memory"

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

def visualize_county_data(county_data, county_name):
    # Convert county names to lowercase and strip whitespace
    county_data_lower = {key.strip().lower(): value for key, value in county_data.items()}
    county_name = county_name.strip().lower()

    county_info = county_data_lower.get(county_name)
    if county_info:
        dates = list(county_info.keys())
        max_aqi_values = [county_info[date]['Max AQI'] for date in dates]

        plt.figure(figsize=(10, 6))

        # Define color thresholds for different AQI ranges
        color_thresholds = {
            'Good': (0, 50),
            'Moderate': (51, 100),
            'Unhealthy for Sensitive Groups': (101, 150),
            'Unhealthy': (151, 200),
            'Very Unhealthy': (201, 300),
            'Hazardous': (301, float('inf'))
        }

        # Define colors for different AQI ranges
        color_map = {
            'Good': 'green',
            'Moderate': 'yellow',
            'Unhealthy for Sensitive Groups': 'orange',
            'Unhealthy': 'red',
            'Very Unhealthy': 'purple',
            'Hazardous': 'maroon'
        }

        # Plot time series with different colors based on AQI values
        for i in range(len(dates) - 1):  # Iterate through dates, except the last one
            current_aqi = max_aqi_values[i]
            next_aqi = max_aqi_values[i + 1]
            current_date = dates[i]
            next_date = dates[i + 1]
            for color, (lower, upper) in color_thresholds.items():
                if lower <= current_aqi <= upper:
                    plt.plot([current_date, next_date], [current_aqi, next_aqi], marker='o', color=color_map[color])
                    break  # Once color is assigned, exit the loop

        plt.title(f"Time Series Plot for {county_name.upper()} AQIs")  # Convert county_name to uppercase for plot title
        plt.xlabel("Date")
        plt.ylabel("Max AQI")
        plt.xticks(rotation=45)
        plt.grid(True)
        plt.tight_layout()
        plt.show()
    else:
        print(f"No data found for {county_name}")

def read_shared_memory(shared_memory_name):

    shm_seg = shared_memory.SharedMemory(name=shared_memory_name)
    data = bytes(shm_seg.buf).strip(b'\x00').decode('ascii')
    shm_seg.close()

    resource_tracker.unregister(shm_seg._name, "shared_memory")

    return parse_county_data(data)

baseline_data = read_shared_memory(SHARED_MEM_NAME_BASELINE)
county_data = read_shared_memory(SHARED_MEM_NAME)

