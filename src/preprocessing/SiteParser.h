#ifndef SITEPARSER_H
#define SITEPARSER_H

#include <string>
#include <vector>

struct SiteInfo
{
    // std::string AQSID;
    // std::string ParameterName;
    // std::string SiteCode;
    // std::string SiteName;
    // std::string Status;
    // std::string AgencyID;
    std::string AgencyName;
    // std::string EPARegion;
    double Latitude;
    double Longitude;
    // double Elevation;
    // double GMTOffset;
    // std::string CountryCode;
    // int MSACode;
    // std::string MSAName;
    // std::string StateCode;
    // std::string StateName;
    // int CountyCode;
    std::string CountyName;
};

std::vector<SiteInfo> parseSiteLocations(const std::string &filename);
bool isInBayArea(const std::string &countyName);

#endif // SITEPARSER_H
