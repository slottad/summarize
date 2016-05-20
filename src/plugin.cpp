/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2016 SciDB, Inc.
* All Rights Reserved.
*
* summarize is a plugin for SciDB, an Open Source Array DBMS maintained
* by Paradigm4. See http://www.paradigm4.com/
*
* summarize is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* summarize is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with summarize.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

//#include <vector>
#include <algorithm>
#include <boost/assign.hpp>

#include <SciDBAPI.h>
#include <system/ErrorsLibrary.h>
#include "query/FunctionDescription.h"

using namespace std;
using namespace scidb;
using namespace boost::assign;

EXPORTED_FUNCTION void GetPluginVersion(uint32_t& major, uint32_t& minor, uint32_t& patch, uint32_t& build)
{
    major = SCIDB_VERSION_MAJOR();
    minor = SCIDB_VERSION_MINOR();
    patch = SCIDB_VERSION_PATCH();
    build = SCIDB_VERSION_BUILD();
}

void human_bytes(const scidb::Value** args, scidb::Value* res, void*) 
{
    int64_t bytes = *(static_cast<int64_t*> (args[0]->data()));
    double dbytes = static_cast<double>(bytes);
    
    const char *suffixes[] = {"bytes", "KB", "MB", "GB", "TB", "PB" };
    size_t suffixes_size = 6;
    size_t rank = static_cast<size_t>(trunc((log10(dbytes)) / 3));
    rank = min(rank, suffixes_size - 1);
    double human = dbytes / pow(1024.0, rank);
    char buf[32];
    sprintf(buf, "%4.2f%s", human, suffixes[rank]);
    res->setString(buf);
}



class Instance
{
public:
    Instance()
    {}

    ~Instance()
    {}

} _instance;


REGISTER_FUNCTION(human_bytes, list_of("int64"), TID_STRING, human_bytes);
