#include <iostream>
#include <sstream>
#include <unordered_map>
#include <vector>
#include <string>
#include <omp.h>
#include <mpi.h>
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "preprocessing/CSVParser.h"
#include "preprocessing/SiteParser.h"

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

// Structure to hold daily data for each county
struct DailyData
{
    int maxAQI;
    std::string maxPollutant;
};

// Function to remove quotes from a string
std::string removeQuotes(const std::string &str)
{
    std::string result = str;
    if (!result.empty() && result.front() == '"')
        result.erase(0, 1);
    if (!result.empty() && result.back() == '"')
        result.pop_back();
    return result;
}

// Function to parse CSV data and filter based on agency name
std::vector<DataPoint> parseAndFilterCSVData(const std::string &fileName, const std::vector<SiteInfo> &sites)
{
    CSVParser parser(fileName);
    std::vector<DataPoint> filteredData;

    if (!parser.parse())
    {
        std::cerr << "Error parsing CSV file: " << fileName << std::endl;
        return filteredData;
    }

    std::vector<std::vector<std::string>> data = parser.getData();

    for (const auto &row : data)
    {
        if (row.size() < 8)
            continue;

        std::string latitudeStr = removeQuotes(row[0]);
        std::string longitudeStr = removeQuotes(row[1]);
        std::string agencyName = removeQuotes(row[10]);
        std::string AQIStr = removeQuotes(row[7]);
        std::string pollutant = removeQuotes(row[3]);

        // Skip rows where AQI is -999
        if (AQIStr == "-999")
            continue;

        // Check if agency name matches the specified ones
        if (agencyName == "Monterey Bay Unified APCD" || agencyName == "San Joaquin Valley Unified APCD" || agencyName == "Yolo-Solano AQMD" || agencyName == "California Air Resources Board" || agencyName == "San Francisco Bay Area AQMD")
        {
            DataPoint point;
            point.latitude = std::stod(latitudeStr);
            point.longitude = std::stod(longitudeStr);
            point.agencyName = agencyName;
            point.AQI = std::stoi(AQIStr);
            point.pollutant = pollutant;

            // Find county name based on latitude and longitude
            for (const auto &site : sites)
            {
                if (site.Latitude == point.latitude && site.Longitude == point.longitude)
                {
                    point.countyName = site.CountyName;
                    if (point.countyName.empty() || point.countyName == "")
                    {
                        std::cout << "County name is empty or blank" << std::endl;
                    }
                    break;
                }
            }

            filteredData.push_back(point);
        }
    }
    for (int i = 0; i < filteredData.size(); i++)
    {
        if (filteredData[i].countyName.empty() || filteredData[i].countyName == "")
        {
            filteredData.erase(filteredData.begin() + i);
            i--;
        }
    }

    return filteredData;
}

// Function to serialize county data to JSON string
std::string serializeCountyDataToJson(const std::unordered_map<std::string, std::unordered_map<std::string, DailyData>> &countyDailyData)
{
    rapidjson::StringBuffer s;
    rapidjson::Writer<rapidjson::StringBuffer> writer(s);

    writer.StartObject();
    for (const auto &countyData : countyDailyData)
    {
        writer.Key(countyData.first.c_str());
        writer.StartObject();
        for (const auto &dayData : countyData.second)
        {
            writer.Key(dayData.first.c_str());
            writer.StartObject();
            writer.Key("maxAQI");
            writer.Int(dayData.second.maxAQI);
            writer.Key("maxPollutant");
            writer.String(dayData.second.maxPollutant.c_str());
            writer.EndObject();
        }
        writer.EndObject();
    }
    writer.EndObject();

    return s.GetString();
}

int main(int argc, char *argv[])
{
    omp_set_num_threads(12);

    MPI_Init(&argc, &argv);

    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    if (world_size != 8)
    {
        std::cerr << "This program must be run with 7 MPI processes." << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    std::vector<std::string> folderNames = {
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200814",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200815",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200816",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200817",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200818",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200819",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200820",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200821",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200822",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200823",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200824",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200825",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200826",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200827",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200828",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200829",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200830",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200831",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200901",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200902",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200903",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200904",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200905",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200906",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200907",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200908",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200909",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200910",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200911",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200912",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200913",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200914",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200915",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200916",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200917",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200918",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200919",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200920",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200921",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200922",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200923",
        "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200924"};

    // Parse site locations
    std::vector<SiteInfo> sites = parseSiteLocations("/Users/moukthika/Desktop/cmpe-275/air_quality_simulation/filtered_file22.csv");

    // Create a data structure to hold daily data for each county
    std::unordered_map<std::string, std::unordered_map<std::string, DailyData>> countyDailyData;

    double start_time = omp_get_wtime(); // Start time measurement

    // Process 0 will store received JSON data
    std::vector<std::string> receivedJSONData(world_size);

    // Each process handles a subset of folders except process 0
    if (world_rank != 0)
    {
        int num_folders = folderNames.size();
        int num_folders_per_process = num_folders / (world_size - 1);
        int start_folder_index = (world_rank - 1) * num_folders_per_process;
        int end_folder_index = start_folder_index + num_folders_per_process;

        // Ensure the last process handles any remaining folders
        if (world_rank == world_size - 1)
        {
            end_folder_index = num_folders;
        }

        // Process folders and generate JSON string
        for (int i = start_folder_index; i < end_folder_index; ++i)
        {
            std::string folder = folderNames[i];

            // List of CSV file names within the folder
            std::vector<std::string> fileNames;
            for (int hour = 1; hour <= 23; hour += 2)
            {
                std::string hourString = (hour < 10 ? "0" : "") + std::to_string(hour);
                size_t lastSlashPos = folder.find_last_of('/');

                // Extract the substring containing the date
                std::string date = folder.substr(lastSlashPos + 1, 8);
                std::string fileName = date + "-" + hourString + ".csv";
                fileNames.push_back(fileName);
            }

            // Process files using OpenMP
#pragma omp parallel for
            for (int j = 0; j < fileNames.size(); ++j)
            {
                std::string fileName = fileNames[j];
                std::string filePath = folder + "/" + fileName;
                std::vector<DataPoint> filteredData = parseAndFilterCSVData(filePath, sites);

                // print process number, thread number and file name
                int thread_num = omp_get_thread_num();
                // std::cout << "Process " << world_rank << ", Thread " << thread_num << " processing file: " << fileName << std::endl;

                // Update daily data for each county
#pragma omp critical
                {
                    std::string day = fileName.substr(0, 8); // Extract day from filename
                    for (const auto &point : filteredData)
                    {
                        countyDailyData[point.countyName][day].maxAQI = std::max(countyDailyData[point.countyName][day].maxAQI, point.AQI);
                        if (countyDailyData[point.countyName][day].maxAQI == point.AQI)
                        {
                            countyDailyData[point.countyName][day].maxPollutant = point.pollutant;
                        }
                    }
                }
            }
        }

        // Serialize county data to JSON string
        std::string jsonData = serializeCountyDataToJson(countyDailyData);

        // Convert JSON string to const char array
        const char *jsonDataChar = jsonData.c_str();
        int jsonDataSize = jsonData.size() + 1;

        // Send serialized data to process 0
        MPI_Send(jsonDataChar, jsonDataSize, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
    }
    else // Process 0 receives and deserializes data
    {
        // Aggregate received JSON data for each county
        for (int source_rank = 1; source_rank < world_size; ++source_rank)
        {
            MPI_Status status;
            MPI_Probe(source_rank, 0, MPI_COMM_WORLD, &status);

            int jsonDataSize;
            MPI_Get_count(&status, MPI_CHAR, &jsonDataSize);

            char *jsonDataChar = new char[jsonDataSize];
            MPI_Recv(jsonDataChar, jsonDataSize, MPI_CHAR, source_rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            // Deserialize received JSON data
            rapidjson::Document document;
            document.Parse(jsonDataChar);

            // Iterate over each county in the received JSON data
            for (auto it = document.MemberBegin(); it != document.MemberEnd(); ++it)
            {
                std::string county = it->name.GetString();

                // Iterate over each date and its corresponding data
                for (auto dayIt = it->value.GetObject().MemberBegin(); dayIt != it->value.GetObject().MemberEnd(); ++dayIt)
                {
                    std::string date = dayIt->name.GetString();
                    int maxAQI = dayIt->value.GetObject()["maxAQI"].GetInt();
                    std::string maxPollutant = dayIt->value.GetObject()["maxPollutant"].GetString();

                    // Aggregate data under the county key
                    countyDailyData[county][date] = {maxAQI, maxPollutant};
                }
            }

            delete[] jsonDataChar;
        }

        // Print the aggregated county data
        for (const auto &countyData : countyDailyData)
        {
            std::cout << "County: " << countyData.first << std::endl;
            for (const auto &dayData : countyData.second)
            {
                std::cout << "Date: " << dayData.first << ", ";
                std::cout << "Max AQI: " << dayData.second.maxAQI << ", ";
                std::cout << "Max Pollutant: " << dayData.second.maxPollutant << std::endl;
            }
        }
    }

    MPI_Finalize();

    double end_time = MPI_Wtime(); // End time measurement
    double elapsed_time = end_time - start_time;

    // Output processing time
    // std::cout << "Process " << world_rank << " took " << elapsed_time << " seconds to process files." << std::endl;

    return 0;
}
