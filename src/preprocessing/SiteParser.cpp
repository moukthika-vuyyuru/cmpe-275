#include "SiteParser.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <unordered_set>

#include "SiteParser.h"
#include <fstream>
#include <sstream>
#include <iostream>

#include "SiteParser.h"
#include <fstream>
#include <sstream>
#include <iostream>

std::vector<SiteInfo> parseSiteLocations(const std::string &filename)
{
    std::vector<SiteInfo> sites;

    std::ifstream file(filename);
    if (!file.is_open())
    {
        std::cerr << "Error: Failed to open file " << filename << std::endl;
        return sites; // Return empty vector if file cannot be opened
    }

    std::string line;
    while (std::getline(file, line))
    {
        std::istringstream iss(line);
        SiteInfo site;

        std::string field;
        size_t count = 0;

        while (std::getline(iss, field, ','))
        {
            switch (count)
            {
            case 0:
                // std::cout << "AgencyName: " << field << std::endl;
                site.AgencyName = field;
                break;
            case 1:
                // std::cout << "Latitude: " << field << std::endl;
                site.Latitude = std::stod(field);
                break;
            case 2:
                // std::cout << "Longitude: " << field << std::endl;
                site.Longitude = std::stod(field);
                break;
            case 3:
                // std::cout << "CountyName: " << field << std::endl;
                site.CountyName = field;
                break;
            default:
                break;
            }
            count++;
        }
        sites.push_back(site);
    }

    file.close();
    return sites;
}