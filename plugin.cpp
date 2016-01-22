
/*
 * @file plugin.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Implementation of some plugin functions.
 */

#include <vector>

#include "SciDBAPI.h"
#include "system/Constants.h"

/**
 * EXPORT FUNCTIONS
 * Functions from this section will be used by LOAD LIBRARY operator.
 */
EXPORTED_FUNCTION void GetPluginVersion(uint32_t& major, uint32_t& minor, uint32_t& patch, uint32_t& build)
{
    major = scidb::SCIDB_VERSION_MAJOR();
    minor = scidb::SCIDB_VERSION_MINOR();
    patch = scidb::SCIDB_VERSION_PATCH();
    build = scidb::SCIDB_VERSION_BUILD();
}
