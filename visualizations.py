import sys
import pandas as pd
import matplotlib.patches as mpatches
from multiprocessing import shared_memory, resource_tracker
import matplotlib.pyplot as plt
from datetime import datetime

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

        # Create a list of tuples (date, AQI) and sort by date
        date_aqi_pairs = [(date, aqi) for date, aqi in zip(dates, max_aqi_values)]
        date_aqi_pairs.sort(key=lambda x: datetime.strptime(x[0], '%Y%m%d'))

        dates_sorted, max_aqi_values_sorted = zip(*date_aqi_pairs)

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
        for i in range(len(dates_sorted) - 1):  # Iterate through dates, except the last one
            current_aqi = max_aqi_values_sorted[i]
            next_aqi = max_aqi_values_sorted[i + 1]
            current_date = datetime.strptime(dates_sorted[i], '%Y%m%d').strftime('%Y %b %d')
            next_date = datetime.strptime(dates_sorted[i + 1], '%Y%m%d').strftime('%Y %b %d')
            for color, (lower, upper) in color_thresholds.items():
                if lower <= current_aqi <= upper:
                    plt.plot([i, i + 1], [current_aqi, next_aqi], marker='o', color=color_map[color])
                    break  # Once color is assigned, exit the loop

        # Set x-axis ticks and labels
        plt.xticks(range(len(dates_sorted)), [datetime.strptime(date, '%Y%m%d').strftime('%Y %b %d') for date in dates_sorted], rotation=45)
        plt.xlim(-0.5, len(dates_sorted) - 1.5)  # Adjust x-axis limits to align ticks with data points

        plt.title(f"Time Series Plot for {county_name.upper()} AQIs")  # Convert county_name to uppercase for plot title
        plt.xlabel("Date")
        plt.ylabel("Max AQI")
        plt.grid(True)
        plt.tight_layout()
        plt.show()
    else:
        print(f"No data found for {county_name}")

'''def visualize_county_data(county_data, county_name):
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
        print(f"No data found for {county_name}")'''

def visualize_pollutant_distribution(county_data, county_name=None):
    if county_name:
        # Count the occurrences of each pollutant for the specified county
        pollutant_counts = {}
        county_info = county_data.get(county_name)
        if county_info:
            for date_info in county_info.values():
                pollutant = date_info['Max Pollutant']
                if pollutant in pollutant_counts:
                    pollutant_counts[pollutant] += 1
                else:
                    pollutant_counts[pollutant] = 1

            # Create a pie chart for the specified county
            plt.figure(figsize=(8, 8))
            plt.pie(pollutant_counts.values(), labels=pollutant_counts.keys(), autopct='%1.1f%%', startangle=140)
            plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
            plt.title(f'Distribution of Pollutants in {county_name}')
            plt.show()
        else:
            print(f"No data found for {county_name}")
    else:
        # Count the occurrences of each pollutant across all counties
        pollutant_counts = {}
        for county_info in county_data.values():
            for date_info in county_info.values():
                pollutant = date_info['Max Pollutant']
                if pollutant in pollutant_counts:
                    pollutant_counts[pollutant] += 1
                else:
                    pollutant_counts[pollutant] = 1

        # Create a pie chart for all counties
        plt.figure(figsize=(8, 8))
        plt.pie(pollutant_counts.values(), labels=pollutant_counts.keys(), autopct='%1.1f%%', startangle=140)
        plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
        plt.title('Distribution of Pollutants Across Counties')
        plt.show()

def visualize_max_aqi_by_county(county_data):
    # Calculate maximum AQI for each county
    max_aqi_values = {county: max(date_info['Max AQI'] for date_info in county_info.values()) for county, county_info in county_data.items()}

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

    # Plotting
    plt.figure(figsize=(12, 6))
    bars = plt.bar(max_aqi_values.keys(), max_aqi_values.values(), color='grey')  # Default color is grey

    # Color the bars based on AQI range
    for county, max_aqi in max_aqi_values.items():
        for color, (lower, upper) in color_thresholds.items():
            if lower <= max_aqi <= upper:
                bars[list(max_aqi_values.keys()).index(county)].set_color(color_map.get(color, 'grey'))  # Use 'grey' if color not found

    plt.xlabel('County')
    plt.ylabel('Maximum AQI Value')
    plt.title('Maximum AQI Values by County')
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y')
    plt.tight_layout()

    # Create custom legend
    legend_patches = [mpatches.Patch(color=color_map[color], label=f'{color}: {threshold[0]}-{threshold[1]}') for color, threshold in color_thresholds.items()]
    plt.legend(handles=legend_patches, title='AQI Range', loc='upper left')

    plt.show()


def read_shared_memory(shared_memory_name):

    shm_seg = shared_memory.SharedMemory(name=shared_memory_name)
    data = bytes(shm_seg.buf).strip(b'\x00').decode('ascii')
    shm_seg.close()

    resource_tracker.unregister(shm_seg._name, "shared_memory")

    return parse_county_data(data)

baseline_data = read_shared_memory(SHARED_MEM_NAME_BASELINE)
county_data = read_shared_memory(SHARED_MEM_NAME)


visualize_county_data(county_data," SANTA CRUZ")
visualize_pollutant_distribution(county_data, " YOLO")  # Show distribution for a specific county
visualize_pollutant_distribution(county_data)  # Show distribution for all counties
visualize_max_aqi_by_county(county_data)