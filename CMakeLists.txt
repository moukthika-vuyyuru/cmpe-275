cmake_minimum_required(VERSION 3.3)
project(hello)

# Set compilers to use (GCC 13.2)
set(CMAKE_C_COMPILER /opt/homebrew/Cellar/gcc/13.2.0/bin/gcc-13)
set(CMAKE_CXX_COMPILER /opt/homebrew/Cellar/gcc/13.2.0/bin/g++-13)

# Add OpenMP flags
set(OPENMP_CXX_FLAGS "-O3 -fopenmp -std=c++11")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OPENMP_CXX_FLAGS}")

# Add MPI flags and libraries
find_package(MPI REQUIRED)
include_directories(${MPI_INCLUDE_PATH})
set(CMAKE_CXX_COMPILE_FLAGS "${CMAKE_CXX_COMPILE_FLAGS} ${MPI_COMPILE_FLAGS}")
set(CMAKE_CXX_LINK_FLAGS "${CMAKE_CXX_LINK_FLAGS} ${MPI_LINK_FLAGS}")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${MPI_EXE_LINKER_FLAGS}")

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
target_compile_features(main PRIVATE cxx_std_17)
target_link_libraries(main ${MPI_LIBRARIES})
# Link Boost libraries
target_link_libraries(main ${Boost_LIBRARIES})
