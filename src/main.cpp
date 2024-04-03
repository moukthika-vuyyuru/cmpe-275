#include <iostream>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <iomanip>
#include <vector>
#include <string>
#include "preprocessing/CSVParser.h"
#include "preprocessing/SiteParser.h"
#include <omp.h> // Include OpenMP header

// Structure to hold data for each data point
struct DataPoint
{
    double latitude;
    double longitude;
    std::string agencyName;
    int AQI;
    std::string pollutant;
    std::string countyName;
};

// Structure to hold hourly data for each county
struct HourlyData
{
    int maxAQI;
    std::string maxPollutant;
};

// Function to parse CSV data and filter based on agency name
std::vector<DataPoint> parseAndFilterCSVData(const std::string &fileName, const std::vector<SiteInfo> &sites)
{
    CSVParser parser(fileName);
    std::vector<DataPoint> filteredData;

    if (parser.parse())
    {
        std::vector<std::vector<std::string>> data = parser.getData();

        for (const auto &row : data)
        {
            if (row.size() >= 8)
            {
                std::string latitudeStr = row[0];
                std::string longitudeStr = row[1];
                std::string agencyName = row[10];
                std::string AQIStr = row[7];
                std::string pollutant = row[3];

                // Skip rows where AQI is -999
                if (AQIStr != "-999")
                {
                    // Remove quotes from strings
                    agencyName.erase(0, 1);
                    agencyName.pop_back();
                    latitudeStr.erase(0, 1);
                    latitudeStr.pop_back();
                    longitudeStr.erase(0, 1);
                    longitudeStr.pop_back();
                    pollutant.erase(0, 1);
                    pollutant.pop_back();

                    // Check if agency name matches the specified ones
                    if (agencyName == "Yolo-Solano AQMD" || agencyName == "California Air Resources Board" || agencyName == "San Francisco Bay Area AQMD")
                    {
                        DataPoint point;
                        point.latitude = std::stod(latitudeStr);
                        point.longitude = std::stod(longitudeStr);
                        point.agencyName = agencyName;
                        // remove quotes from AQI string
                        AQIStr.erase(0, 1);
                        AQIStr.pop_back();
                        point.AQI = std::stoi(AQIStr);
                        point.pollutant = pollutant;

                        // Find county name based on latitude and longitude
                        for (const auto &site : sites)
                        {
                            if (site.Latitude == point.latitude && site.Longitude == point.longitude)
                            {
                                point.countyName = site.CountyName;
                                break;
                            }
                        }

                        filteredData.push_back(point);
                    }
                }
            }
        }
    }
    else
    {
        std::cerr << "Error parsing CSV file: " << fileName << std::endl;
    }

    return filteredData;
}

int main()
{
    // List of odd-hour CSV file names
    std::vector<std::string> fileNames;
    for (int hour = 1; hour <= 23; hour += 2)
    {
        std::string hourString = (hour < 10 ? "0" : "") + std::to_string(hour);
        std::string fileName = "20200901-" + hourString + ".csv";
        fileNames.push_back(fileName);
    }

    // Parse site locations
    std::vector<SiteInfo> sites = parseSiteLocations("/Users/moukthika/Desktop/cmpe-275/air_quality_simulation/filtered_file22.csv");

    // Create a data structure to hold hourly data for each county
    std::unordered_map<std::string, std::unordered_map<std::string, HourlyData>> countyHourlyData;

    // Start time measurement
    double startTime = omp_get_wtime();
#pragma omp parallel
    std::cout << "Number of threads: " << omp_get_num_threads() << std::endl;

    // Parallel processing of CSV files
#pragma omp parallel for
    for (size_t i = 0; i < fileNames.size(); ++i)
    {
        // print thread number
        std::cout << "Thread number: " << omp_get_thread_num() << std::endl;
        double fileStartTime = omp_get_wtime();
        std::vector<DataPoint> filteredData = parseAndFilterCSVData("/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200901/" + fileNames[i], sites);

        // Update hourly data for each county
        for (const auto &point : filteredData)
        {
#pragma omp critical // Ensure mutual exclusion when updating shared data structures
            {
                countyHourlyData[point.countyName][fileNames[i]].maxAQI = std::max(countyHourlyData[point.countyName][fileNames[i]].maxAQI, point.AQI);
                if (countyHourlyData[point.countyName][fileNames[i]].maxAQI == point.AQI)
                {
                    countyHourlyData[point.countyName][fileNames[i]].maxPollutant = point.pollutant;
                }
            }
        }
        double fileEndTime = omp_get_wtime();
        std::cout << "Time taken to process " << fileNames[i] << ": " << fileEndTime - fileStartTime << " seconds" << std::endl;
    }

    // End time measurement
    double endTime = omp_get_wtime();

    // Print total time taken
    std::cout << "Total time taken: " << endTime - startTime << " seconds" << std::endl;

    // Print hourly, county-wise max AQI and pollutant
    for (const auto &countyData : countyHourlyData)
    {
        std::cout << "County: " << countyData.first << std::endl;
        for (const auto &hourlyData : countyData.second)
        {
            std::cout << "Hour: " << hourlyData.first << ", Max AQI: " << hourlyData.second.maxAQI << ", Pollutant: " << hourlyData.second.maxPollutant << std::endl;
        }
    }

    return 0;
}
