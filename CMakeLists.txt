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

# shared library for user defined objects
set (operators_src
    plugin.cpp
    LogicalStub.cpp
    PhysicalStub.cpp
)

add_library(operators SHARED ${operators_src})
set_target_properties(operators PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}/plugins)

