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

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <query/Operator.h>
#include <log4cxx/logger.h>

#ifndef FASTCOUNT_SETTINGS
#define FASTCOUNT_SETTINGS

#ifdef CPP11
using std::shared_ptr;
#else
using boost::shared_ptr;
#endif

using boost::algorithm::trim;
using boost::starts_with;
using boost::lexical_cast;
using boost::bad_lexical_cast;
using namespace std;

namespace scidb
{

class FastCountSettings
{
private:

public:
    static const size_t MAX_PARAMETERS = 2;

    FastCountSettings(vector<shared_ptr<OperatorParam> > const& operatorParameters,
                    bool logical,
                    shared_ptr<Query>& query)
    {

    	size_t const nParams = operatorParameters.size();

        vector<string>     filePaths;
        vector<InstanceID> instanceIds;

    	if (nParams > MAX_PARAMETERS)
    	{   //assert-like exception. Caller should have taken care of this!
    		throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal number of parameters passed to FastCountSettings";
    	}
    	for (size_t i= 0; i<nParams; ++i)
    	{
    		shared_ptr<OperatorParam>const& param = operatorParameters[i];
    		string parameterString;
    		if (logical)
    		{
    			parameterString = evaluate(((shared_ptr<OperatorParamLogicalExpression>&) param)->getExpression(),query, TID_STRING).getString();
    		}
    		else
    		{
    			parameterString = ((shared_ptr<OperatorParamPhysicalExpression>&) param)->getExpression()->evaluate().getString();
    		}
        }
    }

    };

}


#endif //FastCountSettings
