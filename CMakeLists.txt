cmake_minimum_required(VERSION 3.15)

project(SortsBenchmark)

# set(CMAKE_C_COMPILER "/usr/local/bin/clang")
# set(CMAKE_CXX_COMPILER "/usr/local/bin/clang++")

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED true)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_FLANGS "-Wall -Wextra -O3 march=native")

find_package(benchmark REQUIRED)
find_package(Arrow REQUIRED)

add_subdirectory(src)