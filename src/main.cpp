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
#include <boost/tokenizer.hpp>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>
#include "python.h"

#define SHARED_MEM_NAME "/county_data_shared_memory"
#define SHARED_MEM_NAME_BASELINE "/shared_memory_baseline"

struct DataPoint
{
    double latitude;
    double longitude;
    std::string agencyName;
    int AQI;
    std::string pollutant;
    std::string countyName;
};

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
void parseAndFilterCSVData(const std::string &fileName, const std::vector<SiteInfo> &sites, std::unordered_map<std::string, std::unordered_map<std::string, DailyData>> &localCountyDailyData)
{
    std::ifstream file(fileName);
    if (!file.is_open())
    {
        std::cerr << "Error opening file: " << fileName << std::endl;
        return;
    }

    using namespace boost;
    typedef tokenizer<escaped_list_separator<char>> Tokenizer;
    std::string date = fileName.substr(fileName.find_last_of('/') + 1, 8);

    std::string line;
    while (std::getline(file, line))
    {
        Tokenizer tok(line);
        std::vector<std::string> row;
        for (const auto &token : tok)
        {
            row.push_back(token);
        }

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
                    // update local county data structure
                    localCountyDailyData[site.CountyName][date].maxAQI = std::max(localCountyDailyData[site.CountyName][date].maxAQI, point.AQI);
                    if (localCountyDailyData[site.CountyName][date].maxAQI == point.AQI)
                    {
                        localCountyDailyData[site.CountyName][date].maxPollutant = point.pollutant;
                    }
                    break;
                }
            }
        }
    }

    file.close();
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
        writer.StartArray();
        for (const auto &dayData : countyData.second)
        {
            writer.StartObject();
            writer.Key("date");
            writer.String(dayData.first.c_str());
            writer.Key("maxAQI");
            writer.Int(dayData.second.maxAQI);
            writer.Key("maxPollutant");
            writer.String(dayData.second.maxPollutant.c_str());
            writer.EndObject();
        }
        writer.EndArray();
    }
    writer.EndObject();

    return s.GetString();
}

void unlinkSharedMemoryIfExists(const std::string &sharedMemName)
{
    int shm_fd = shm_open(sharedMemName.c_str(), O_RDWR, 0);
    if (shm_fd == -1)
    {
        // Check if the error is due to the segment not existing
        if (errno != ENOENT)
        {
            std::cerr << "Failed to open shared memory: " << sharedMemName << std::endl;
            perror("shm_open");
        }
        return;
    }

    close(shm_fd);

    if (shm_unlink(sharedMemName.c_str()) == -1)
    {
        std::cerr << "Failed to unlink shared memory: " << sharedMemName << std::endl;
        perror("shm_unlink");
        return;
    }
}

void writeToSharedMemory(const std::string &data, const std::string &sharedMemName)
{
    int shm_fd = shm_open(sharedMemName.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (shm_fd == -1)
    {
        std::cerr << "Failed to open shared memory" << std::endl;
        return;
    }

    if (ftruncate(shm_fd, data.size() + 1) == -1)
    {
        perror("ftruncate");
    }

    void *ptr = mmap(0, data.size() + 1, PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (ptr == MAP_FAILED)
    {
        std::cerr << "Memory mapping failed" << std::endl;
        return;
    }

    std::memcpy(ptr, data.c_str(), data.size() + 1);

    close(shm_fd);
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
        std::cerr << "This program must be run with 8 MPI processes." << std::endl;
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
    std::vector<SiteInfo> sites = parseSiteLocations("/Users/moukthika/Desktop/air-now/cmpe-275/monitoring_site_locations.csv");

    // Create a data structure to hold daily data for each county
    std::unordered_map<std::string, std::unordered_map<std::string, DailyData>> countyDailyData;

    double start_time = MPI_Wtime();

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
                std::unordered_map<std::string, std::unordered_map<std::string, DailyData>> localCountyDailyData;
                parseAndFilterCSVData(filePath, sites, localCountyDailyData);

                // Merge local data into global data structure using critical section
#pragma omp critical
                {
                    for (const auto &countyData : localCountyDailyData)
                    {
                        for (const auto &dayData : countyData.second)
                        {
                            countyDailyData[countyData.first][dayData.first].maxAQI = std::max(countyDailyData[countyData.first][dayData.first].maxAQI, dayData.second.maxAQI);
                            if (countyDailyData[countyData.first][dayData.first].maxAQI == dayData.second.maxAQI)
                            {
                                countyDailyData[countyData.first][dayData.first].maxPollutant = dayData.second.maxPollutant;
                            }
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

        // process aug 10th data first to get the baseline and write to shared memory and then wait for other processes to send data
        std::string folder = "/Users/moukthika/Desktop/cmpe-275/airnow-2020fire/data/20200810";

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
            std::unordered_map<std::string, std::unordered_map<std::string, DailyData>> localCountyDailyData;
            parseAndFilterCSVData(filePath, sites, localCountyDailyData);

// Merge local data into global data structure using critical section
#pragma omp critical
            for (const auto &countyData : localCountyDailyData)
            {
                for (const auto &dayData : countyData.second)
                {
                    countyDailyData[countyData.first][dayData.first].maxAQI = std::max(countyDailyData[countyData.first][dayData.first].maxAQI, dayData.second.maxAQI);
                    if (countyDailyData[countyData.first][dayData.first].maxAQI == dayData.second.maxAQI)
                    {
                        countyDailyData[countyData.first][dayData.first].maxPollutant = dayData.second.maxPollutant;
                    }
                }
            }
        }

        std::ostringstream oss_aug10;

        // Iterate over each county in the received data and write it to a string stream
        for (const auto &countyData : countyDailyData)
        {
            oss_aug10 << "County: " << countyData.first << std::endl;
            for (const auto &dayData : countyData.second)
            {
                oss_aug10 << "Date: " << dayData.first << ", ";
                oss_aug10 << "Max AQI: " << dayData.second.maxAQI << ", ";
                oss_aug10 << "Max Pollutant: " << dayData.second.maxPollutant << std::endl;
            }
        }

        std::string data_aug10 = oss_aug10.str();
        unlinkSharedMemoryIfExists(SHARED_MEM_NAME_BASELINE);

        writeToSharedMemory(data_aug10, SHARED_MEM_NAME_BASELINE);

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

            for (auto it = document.MemberBegin(); it != document.MemberEnd(); ++it)
            {
                std::string county = it->name.GetString();

                // Iterate over each date and its corresponding data
                for (auto dayIt = it->value.GetArray().Begin(); dayIt != it->value.GetArray().End(); ++dayIt)
                {
                    std::string date = (*dayIt)["date"].GetString();
                    int maxAQI = (*dayIt)["maxAQI"].GetInt();
                    std::string maxPollutant = (*dayIt)["maxPollutant"].GetString();

                    // Aggregate data under the county key
                    countyDailyData[county][date] = {maxAQI, maxPollutant};
                }
            }

            delete[] jsonDataChar;
        }

        std::ostringstream oss;

        // Iterate over each county in the received data and write it to a string stream
        for (const auto &countyData : countyDailyData)
        {
            oss << "County: " << countyData.first << std::endl;
            for (const auto &dayData : countyData.second)
            {
                oss << "Date: " << dayData.first << ", ";
                oss << "Max AQI: " << dayData.second.maxAQI << ", ";
                oss << "Max Pollutant: " << dayData.second.maxPollutant << std::endl;
            }
        }

        std::string data = oss.str();

        // Write aggregated data to shared memory
        unlinkSharedMemoryIfExists(SHARED_MEM_NAME);
        writeToSharedMemory(data, SHARED_MEM_NAME);

        //   Create your own virtual environment and install matplotlib
        setenv("PYTHONPATH", "/Users/moukthika/Desktop/MiniProject2_275/cmpe-275/venv/lib/python3.12/site-packages", 1);
        Py_Initialize();
        PyObject *sysPath = PySys_GetObject((char *)"path");
        PyList_Append(sysPath, PyUnicode_FromString("/Users/moukthika/Desktop/CMPE-275/cmpe-275/src"));

        const char *scriptName = "visualizations";
        PyObject *pModule = PyImport_ImportModule(scriptName);
        if (pModule != nullptr)
        {
            std::cout << "Module imported successfully." << std::endl;
        }
        else
        {
            PyErr_Print();
            fprintf(stderr, "Failed to load \"%s\"\n", scriptName);
        }
        Py_Finalize();
    }

    MPI_Finalize();

    double end_time = MPI_Wtime(); // End time measurement
    double elapsed_time = end_time - start_time;

    // Output processing time
    std::cout << "Process " << world_rank << " took " << elapsed_time << " seconds to process files." << std::endl;

    return 0;
}
