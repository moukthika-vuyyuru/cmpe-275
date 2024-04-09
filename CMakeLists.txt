cmake_minimum_required(VERSION 3.3)
project(hello)

# Set compilers to use (GCC 13.2)
set(CMAKE_C_COMPILER /opt/homebrew/Cellar/gcc/13.2.0/bin/gcc-13)
set(CMAKE_CXX_COMPILER /opt/homebrew/Cellar/gcc/13.2.0/bin/g++-13)

# Add OpenMP flags
set(OPENMP_CXX_FLAGS "-O3 -fopenmp -std=c++11")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OPENMP_CXX_FLAGS}")

# Manually set MPI libraries
set(MPI_LIBRARIES
    /opt/homebrew/Cellar/open-mpi/5.0.2_1/lib/libmpi.dylib
    /opt/homebrew/Cellar/open-mpi/5.0.2_1/lib/libmpi_mpifh.dylib
    /opt/homebrew/Cellar/open-mpi/5.0.2_1/lib/libmpi_usempi_ignore_tkr.dylib
    /opt/homebrew/Cellar/open-mpi/5.0.2_1/lib/libmpi_usempif08.dylib
)

# Ensure MPI_INCLUDE_PATH is set correctly
set(MPI_INCLUDE_PATH /opt/homebrew/Cellar/open-mpi/5.0.2_1/include)

# Check if MPI_INCLUDE_PATH exists
if(NOT EXISTS ${MPI_INCLUDE_PATH})
    message(FATAL_ERROR "MPI_INCLUDE_PATH does not exist: ${MPI_INCLUDE_PATH}")
endif()

# Include MPI directories
include_directories(${MPI_INCLUDE_PATH})

# Set the path to RapidJSON include directory
set(RAPIDJSON_INCLUDE_DIR /opt/homebrew/Cellar/rapidjson/1.1.0/include)

# Find Boost
find_package(Boost REQUIRED COMPONENTS system filesystem)

# Include RapidJSON directory
include_directories(${RAPIDJSON_INCLUDE_DIR})

# Include Boost directories
include_directories(${Boost_INCLUDE_DIRS})

# Add source files
set(SOURCES
    src/main.cpp
    src/preprocessing/CSVParser.cpp
    src/preprocessing/SiteParser.cpp
)

# Add executable target
add_executable(main ${SOURCES})

# Set C++17 standard
target_compile_features(main PRIVATE cxx_std_17)

# Link MPI libraries
target_link_libraries(main ${MPI_LIBRARIES})

# Link Boost libraries
target_link_libraries(main ${Boost_LIBRARIES})

find_package(Python3 COMPONENTS Development)

if(Python3_FOUND)
    include_directories(${Python3_INCLUDE_DIRS})
    target_link_libraries(main ${Python3_LIBRARIES})
else()
    message(FATAL_ERROR "Python3 not found")
endif()