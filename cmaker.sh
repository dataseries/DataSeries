#!/bin/bash

# This script allows the user to build DataSeries without having to modify the
# cmake files. Use this to set cmake variables to appropriate values for your
# system and preferences. It should be helpful if cmake is failing to find
# necessary files, for example with local installations of Lintel. See
# http://www.cmake.org/Wiki/CMake_Useful_Variables for a list of variables not
# mentioned below that you may want to set. 

# Be careful: any variables set here will override those set in cmake files
# with the same name.

### Usage

# cmaker.sh [DataSeries-path] [options]

# Set the variables you want to change between the solid lines below. These
# will be automatically passed to cmake as command-line options. Give the path
# to the DataSeries directory as the first argument. Additional arguments to
# this script will be directly passed to cmake as command line options.

# Example:

# cmaker ../DataSeries -DSNAPPY_FIND_REQUIRED=ON -DCMAKE_CXX_COMPILER=gcc-4.7.3


#################################################################

## CMake doesn't need these variables, but you can set them here for use in
## setting the script variables

# Gets the absolute path of the DataSeries source directory in case a relative
# path was given
DATASERIES_DIR="`pwd`/${1}"

# Location of Lintel source directory
Lintel_DIR="$~/projects/Lintel"

# Location of the Lintel installation
Lintel_INSTALL_DIR="/usr/local/share"

################################################################

# Find and output all pre-existing environment variables.
tdir=`mktemp -d cmaker.XXXXXXXXXX`
trap "echo "trap1"; rm -rf $tdir; exit 1" HUP INT TERM
trap "rm -rf $tdir; exit 0" EXIT PIPE
(set -o posix ; set) > $tdir/before

############### For Local Installations of Lintel ###############
#
#  LINTEL_INCLUDE_DIR         - where to find Lintel/AssertBoost.hpp                 
#  LINTEL_LIBRARIES           - List of libraries when using LINTEL
#
#  LINTELPTHREAD_INCLUDE_DIR  - where to find Lintel/PThread.hpp      
#  LINTELPTHREAD_LIBRARIES    - list of libraries when using LINTELPTHREAD
#
#  LINTEL_CONFIG_EXTRA_PATHS  - additional locations to search for lintel-config

LINTEL_INCLUDE_DIR="${Lintel_INSTALL_DIR}/include/"
LINTEL_LIBRARIES="${Lintel_INSTALL_DIR}/src"

LINTELPTHREAD_INCLUDE_DIR="${Lintel_INSTALL_DIR}/include/"
LINTELPTHREAD_LIBRARIES="${Lintel_INSTALL_DIR}/src"

LINTEL_CONFIG_EXTRA_PATHS="${Lintel_INSTALL_DIR}/src"
LINTEL_LATEX_REBUILD_EXTRA_PATHS="${Lintel_INSTALL_DIR}/src"

## Linker flags
CMAKE_CXX_LINK_FLAGS="-lrt -L ${Lintel_INSTALL_DIR}/src"
CMAKE_C_LINK_FLAGS="${CMAKE_CXX_LINK_FLAGS}"

######################## Compiler Options ########################

# To avoid cache conflicts with these variables, run in a clean directory 

# Anecdotal testing on a 64-core Opteron machine (limited to 32 total threads of
# DataSeries) showed about 50% speedup with O3 as compared with O2.
CMAKE_CXX_FLAGS="-g -O3"

#CMAKE_C_COMPILER="CC=gcc-4.9.4"
#CMAKE_CXX_COMPILER="g++-4.9.4"

###################### For Extra Programs ######################
#
#  Variable_ENABLED              - true if VARIABLE is enabled
#  Variable_INCLUDE_DIR          - where to find header file for Variable
#  Variable_INCLUDES             - all includes needed for Variable
#  Variable_LIBRARY              - where to find the library for Variable
#  Variable_LIBRARIES            - all libraries needed for Variable
#  Variable_FIND_REQUIRED        - set to ON for more info if Variable not found
#  Variable_EXTRA_LIBRARY_PATHS  - extra places to look for the Variable library

LZ4_INCLUDE_DIR="${Lintel_DIR}/../LZ4"
LZ4_EXTRA_LIBRARY_PATHS="${Lintel_DIR}/../LZ4/cmake"

################################################################

# Find all the environment variables, including those set in this script, and
# output them.
(set -o posix ; set) > $tdir/after

# Take the diff of the pre-existing and post-script variables and output it
# after replacing occurrences of ' with " (cmake can't interpret single quotes)
`diff $tdir/before $tdir/after |tr "\'" "\"" > /tmp/variables.diff`

# Go through the diff output, and for each line that begins with a '>', strip
# off the leading "> " string and replace it with " -D".  Then delete all
# newline characters from the output.
`awk '$1 == ">" {sub(/^> /, " -D") ;  print }' /tmp/variables.diff | tr -d "\n" > /tmp/variables.formatted`

eval "cmake$(cat /tmp/variables.formatted) ${@:2} ${DATASERIES_DIR}"
