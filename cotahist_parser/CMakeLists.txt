# CMakeList.txt : CMake project for cotahist_parser, include source and define
# project specific logic here.
#
cmake_minimum_required (VERSION 3.8)

# Add source to this project's executable.
add_executable (cotahist_parser "cotahist_parser.cpp" "sqlite3.c" "cotahist_parser.h" )


# TODO: Add tests and install targets if needed.
