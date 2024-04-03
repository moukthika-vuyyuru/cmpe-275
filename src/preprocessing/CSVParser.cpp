// CSVParser.cpp

#include "CSVParser.h"
#include <fstream>
#include <sstream>
#include <iostream>

CSVParser::CSVParser(const std::string &filename) : filename(filename) {}

bool CSVParser::parse()
{
    std::ifstream file(filename);
    if (!file.is_open())
    {
        return false;
    }

    // Read the data
    std::string line;
    while (std::getline(file, line))
    {
        std::vector<std::string> row;
        std::stringstream ss(line);
        std::string item;

        while (std::getline(ss, item, ','))
        {
            row.push_back(item);
        }
        data.push_back(row);
    }
    std::cout << "Data size: " << data.size() << std::endl;
    file.close();
    return true;
}

std::vector<std::vector<std::string>> CSVParser::getData() const
{
    return data;
}
