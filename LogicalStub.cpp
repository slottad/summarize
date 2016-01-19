/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2015 SciDB, Inc.
* All Rights Reserved.
*
* This file is part of the Paradigm4 Enterprise SciDB distribution kit
* and may only be used with a valid Paradigm4 contract and in accord
* with the terms and conditions specified by that contract.
*
* END_COPYRIGHT
*/

/*
 * @file LogicalStub.cpp
 * @author roman.simakov@gmail.com
 *
 * @brief Stub for writing plugins with logical operators
 */

#include "query/Operator.h"

namespace scidb
{

class LogicalStub : public LogicalOperator
{
public:
    LogicalStub(const std::string& logicalName, const std::string& alias):
	    LogicalOperator(logicalName, alias)
	{
        /**
         * See built-in operators implementation for example
         */
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, std::shared_ptr<Query> query)
    {
        /**
         * See built-in operators implementation for example
         */
        return ArrayDesc();
	}

};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalStub, "stub");

} //namespace
