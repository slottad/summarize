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
 * @file PhysicalStub.cpp
 * @author roman.simakov@gmail.com
 *
 * @brief Stub for writing plugins with physical operators
 */

#include "query/Operator.h"

namespace scidb
{

class PhysicalStub: public PhysicalOperator
{
public:
    PhysicalStub(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
	    PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

    std::shared_ptr<Array> execute(std::vector<std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
	{
        /**
         * See built-in operators implementation for example
         */
        return std::shared_ptr<Array>();
	}
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalStub, "stub", "stub_impl");

} //namespace
