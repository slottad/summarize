########################################
# BEGIN_COPYRIGHT
#
# Copyright (C) 2008-2015 SciDB, Inc.
# All Rights Reserved.
#
# This file is part of the Paradigm4 Enterprise SciDB distribution kit
# and may only be used with a valid Paradigm4 contract and in accord
# with the terms and conditions specified by that contract.
#
# END_COPYRIGHT
########################################

add_definitions(-DCPP11)

# shared library for user defined objects
set (summarize_src
    src/plugin.cpp
    src/LogicalSummarize.cpp
    src/PhysicalSummarize.cpp
)

add_library(summarize SHARED ${summarize_src})
set_target_properties(summarize PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}/plugins)
