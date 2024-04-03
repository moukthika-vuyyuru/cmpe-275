#ifndef CSV_PARSER_H
#define CSV_PARSER_H

#include <string>
#include <vector>
#include <fstream>
#include <sstream>

class CSVParser
{
public:
    CSVParser(const std::string &filename);
    bool parse();
    std::vector<std::vector<std::string>> getData() const;
    std::vector<std::string> getHeader() const;

private:
    std::string filename;
    std::vector<std::vector<std::string>> data;

    std::vector<std::string> header;
};

#endif // CSV_PARSER_H
