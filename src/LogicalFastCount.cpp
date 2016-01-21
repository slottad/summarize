/*
**
* BEGIN_COPYRIGHT
*
* PARADIGM4 INC.
* This file is part of the Paradigm4 Enterprise SciDB distribution kit
* and may only be used with a valid Paradigm4 contract and in accord
* with the terms and conditions specified by that contract.
*
* Copyright (C) 2010 - 2015 Paradigm4 Inc.
* All Rights Reserved.
*
* END_COPYRIGHT
*/


#include <query/Operator.h>

#include "FastCountSettings.h"

namespace scidb
{

class LogicalFastCount : public  LogicalOperator
{
public:
    LogicalFastCount(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT();
        //ADD_PARAM_VARIES();
    }

    std::vector<shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        if (_parameters.size() < FastCountSettings::MAX_PARAMETERS)
        {
            res.push_back(PARAM_CONSTANT("string"));
        }
        return res;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        ArrayDesc const& inputSchema = schemas[0];
        FastCountSettings settings (_parameters, true, query);
        vector<DimensionDesc> dimensions(1);
        size_t const nInstances = query->getInstancesCount();
        dimensions[0] = DimensionDesc("i",    0, 0, CoordinateBounds::getMax(), CoordinateBounds::getMax(), 1, 0);
       // dimensions[0] = DimensionDesc("i", 0, 0, nInstances-1, nInstances-1, 1, 0);
        vector<AttributeDesc> attributes;
        attributes.push_back(AttributeDesc((AttributeID)0, "count", TID_UINT64, AttributeDesc::IS_NULLABLE, 0));
        return ArrayDesc("fast_count", attributes, dimensions, defaultPartitioning());
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalFastCount, "fast_count");

} // emd namespace scidb
