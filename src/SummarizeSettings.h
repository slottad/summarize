/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2016 SciDB, Inc.
* All Rights Reserved.
*
* grouped_aggregate is a plugin for SciDB, an Open Source Array DBMS maintained
* by Paradigm4. See http://www.paradigm4.com/
*
* grouped_aggregate is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* grouped_aggregate is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with summarize.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#ifndef SUMMARIZE_SETTINGS
#define SUMMARIZE_SETTINGS

#include <limits>
#include <sstream>
#include <memory>
#include <string>
#include <vector>
#include <ctype.h>

#include <query/Operator.h>
#include <query/AttributeComparator.h>
#include <util/Platform.h>
#include <util/Network.h>

#include <boost/algorithm/string.hpp>
#include <boost/unordered_map.hpp>
#include <boost/lexical_cast.hpp>

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/serialization/list.hpp>
#include <boost/serialization/vector.hpp>

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.summarize"));

namespace summarize
{

using std::string;
using std::vector;
using std::shared_ptr;
using std::dynamic_pointer_cast;
using std::ostringstream;
using boost::algorithm::trim;
using boost::starts_with;
using boost::lexical_cast;
using boost::bad_lexical_cast;

/*
 * Settings for the summarize operator.
 */
class Settings
{
private:
    size_t _numInputAttributes;
    size_t _numInstances;
    bool _perAttributeSet;
    bool _perAttribute;
    bool _perInstanceSet;
    bool _perInstance;

public:
    static const size_t MAX_PARAMETERS = 2;
    Settings(ArrayDesc const& inputSchema,
             vector< shared_ptr<OperatorParam> > const& operatorParameters,
             bool logical,
             shared_ptr<Query>& query):
        _numInputAttributes(inputSchema.getAttributes().size()),
        _numInstances(query->getInstancesCount()),
		_perAttributeSet(false),
		_perAttribute(false),
		_perInstanceSet(false),
		_perInstance(false)

    {
    	string const perAttributeParamHeader              = "per_attribute=";
    	string const perInstanceParamHeader               = "per_instance=";
        size_t const nParams = operatorParameters.size();
         if (nParams > MAX_PARAMETERS)
         {   //assert-like exception. Caller should have taken care of this!
             throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                   << "illegal number of parameters passed to Settings";
         }
        for(size_t i = 0; i<operatorParameters.size(); ++i)
    	{
        	shared_ptr<OperatorParam>const& param = operatorParameters[i];

        	{
        		string parameterString;
        		if (logical)
        		{
        			parameterString = evaluate(((shared_ptr<OperatorParamLogicalExpression>&) param)->getExpression(),query, TID_STRING).getString();
        		}
        		else
        		{
        			parameterString = ((shared_ptr<OperatorParamPhysicalExpression>&) param)->getExpression()->evaluate().getString();
        		}
        		parseStringParam(parameterString);
        	}

    	}
        if (_perInstance == true && _perAttribute == true)
           {   //assert-like exception. Caller should have taken care of this!
               throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                     << "cannot set both per_instance aggregation and per_attribute aggregation equal to true";
            }
    }
private:
    bool checkSizeTParam(string const& param, string const& header, size_t& target, bool& setFlag)
       {
           string headerWithEq = header + "=";
           if(starts_with(param, headerWithEq))
           {
               if(setFlag)
               {
                   ostringstream error;
                   error<<"illegal attempt to set "<<header<<" multiple times";
                   throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
               }
               string paramContent = param.substr(headerWithEq.size());
               trim(paramContent);
               try
               {
                   int64_t val = lexical_cast<int64_t>(paramContent);
                   if(val<=0)
                   {
                       ostringstream error;
                       error<<header<<" must be positive";
                       throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
                   }
                   target = val;
                   setFlag = true;
                   return true;
               }
               catch (bad_lexical_cast const& exn)
               {
                   ostringstream error;
                   error<<"could not parse "<<error.str().c_str();
                   throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
               }
           }
           return false;
       }
    bool checkBoolParam(string const& param, string const& header, bool& target, bool& setFlag)
       {
           string headerWithEq = header + "=";
           if(starts_with(param, headerWithEq))
           {
               if(setFlag)
               {
                   ostringstream error;
                   error<<"illegal attempt to set "<<header<<" multiple times";
                   throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
               }
               string paramContent = param.substr(headerWithEq.size());
               trim(paramContent);
               try
               {
                   target= lexical_cast<bool>(paramContent);
                   setFlag = true;
                   return true;
               }
               catch (bad_lexical_cast const& exn)
               {
                   ostringstream error;
                   error<<"could not parse "<<error.str().c_str();
                   throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
               }
           }
           return false;
       }
    void parseStringParam(string const& param)
    {
    	if(checkBoolParam (param,   "per_attribute",       _perAttribute,          _perAttributeSet       ) ) { return; }
    	if(checkBoolParam (param,   "per_instance",        _perInstance,           _perInstanceSet       ) ) { return; }
    	ostringstream error;
    	error<<"unrecognized parameter "<<param;
    	throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
    }
public:
    ArrayDesc getSchema(shared_ptr<Query>& query)
    {
        vector<DimensionDesc> dimensions(2);
        dimensions[0] = DimensionDesc("instance_id",  0, 0, _numInstances-1,       _numInstances-1,       1,                   0);
        dimensions[1] = DimensionDesc("attribute_id", 0, 0, _numInputAttributes-1, _numInputAttributes-1, _numInputAttributes, 0);
        vector<AttributeDesc> attributes;
        attributes.push_back(AttributeDesc((AttributeID) 0, "att_name",    TID_STRING, AttributeDesc::IS_NULLABLE, 0));
        attributes.push_back(AttributeDesc((AttributeID) 1, "total_count", TID_UINT64, AttributeDesc::IS_NULLABLE, 0));
        attributes.push_back(AttributeDesc((AttributeID) 2, "total_bytes", TID_UINT64, AttributeDesc::IS_NULLABLE, 0));
        attributes.push_back(AttributeDesc((AttributeID) 3, "num_chunks",  TID_UINT64, AttributeDesc::IS_NULLABLE, 0));
        attributes.push_back(AttributeDesc((AttributeID) 4, "min_count",   TID_UINT64, AttributeDesc::IS_NULLABLE, 0));
        attributes.push_back(AttributeDesc((AttributeID) 5, "avg_count",   TID_DOUBLE, AttributeDesc::IS_NULLABLE, 0));
        attributes.push_back(AttributeDesc((AttributeID) 6, "max_count",   TID_UINT64, AttributeDesc::IS_NULLABLE, 0));
        attributes.push_back(AttributeDesc((AttributeID) 7, "min_bytes",   TID_UINT64, AttributeDesc::IS_NULLABLE, 0));
        attributes.push_back(AttributeDesc((AttributeID) 8, "avg_bytes",   TID_DOUBLE, AttributeDesc::IS_NULLABLE, 0));
        attributes.push_back(AttributeDesc((AttributeID) 9, "max_bytes",   TID_UINT64, AttributeDesc::IS_NULLABLE, 0));
        attributes = addEmptyTagAttribute(attributes);
        return ArrayDesc("summarize", attributes, dimensions, defaultPartitioning(), query->getDefaultArrayResidency());
    }

    static const size_t NUM_OUTPUT_ATTRIBUTES = 10;

    size_t numInputAttributes() const
    {
        return _numInputAttributes;
    }

    bool perAttributeflag() const
    {
        return _perAttribute;
    }

    bool perInstanceflag() const
    {
        return _perInstance;
    }

};

struct SummaryTuple
{
private:
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive & ar, const unsigned int /*version*/)
  {
	   ar & attName;
	   ar & totalCount;
	   ar & totalBytes;
	   ar & numChunks;
	   ar & minChunkCount;
	   ar & maxChunkCount;
	   ar & minChunkBytes;
	   ar & maxChunkBytes;
  }
public:
	string attName;
    ssize_t totalCount;
    ssize_t totalBytes;
    ssize_t numChunks;
    ssize_t minChunkCount;
    ssize_t maxChunkCount;
    ssize_t minChunkBytes;
    ssize_t maxChunkBytes;

    SummaryTuple(string att = ""):
        attName(att),
        totalCount(0),
        totalBytes(0),
        numChunks(0),
        minChunkCount(std::numeric_limits<ssize_t>::max()),
        maxChunkCount(-1),
        minChunkBytes(std::numeric_limits<ssize_t>::max()),
        maxChunkBytes(-1)
    {}
};


struct InstanceSummary
{
    InstanceID myInstanceId;
    vector<SummaryTuple> tuples;
    vector<SummaryTuple> aggSummary;
    InstanceSummary(InstanceID iid,
                    size_t const numAttributes,
                    vector<string> attNames):
        myInstanceId(iid),
        tuples(numAttributes,SummaryTuple())
    {
        for(size_t i =0; i<numAttributes; ++i)
        {
            tuples[i].attName=attNames[i];
        }
    }

    void addChunkData(AttributeID attId, ssize_t chunkBytes, ssize_t chunkCount)
    {
        SummaryTuple& tuple = tuples[attId];
        tuple.totalBytes+=chunkBytes;
        tuple.totalCount+=chunkCount;
        tuple.numChunks++;
        if(chunkCount < tuple.minChunkCount)
        {
            tuple.minChunkCount = chunkCount;
        }
        if(chunkCount > tuple.maxChunkCount)
        {
            tuple.maxChunkCount = chunkCount;
        }
        if(chunkBytes < tuple.minChunkBytes)
        {
            tuple.minChunkBytes = chunkBytes;
        }
        if(chunkBytes > tuple.maxChunkBytes)
        {
            tuple.maxChunkBytes = chunkBytes;
        }
    }

    bool finalSummary(Settings const&settings, ArrayDesc const& schema, shared_ptr<Query>& query)
    {
    	size_t numAtt = Settings::NUM_OUTPUT_ATTRIBUTES;
    	InstanceID const myId    = query->getInstanceID();
    	InstanceID const coordId = 0;

    	size_t const numInstances = query->getInstancesCount();
    	bool perAtt = settings.perAttributeflag();
    	bool perIns = settings.perInstanceflag();

    	if(perAtt){aggSummary.reserve(numAtt);}
    	if(perIns){aggSummary.reserve(numInstances);}

    	shared_ptr<SharedBuffer> buf;
        //aggregate the attributes per instance. This requires the instances to send to the
    	//coordinator to combine the results
    	//These results overwrite
    	if(perAtt)
    	{

    		if(myId != coordId)
    		{
    			std::stringstream out;
    			// serialize into the stream
    			boost::archive::binary_oarchive oa(out);
    			oa << tuples;

    			out.seekg(0, std::ios::end);
    			size_t bufSize = out.tellg();
    			out.seekg(0,std::ios::beg);

    			auto tmp = out.str();
    			const char* cstr = tmp.c_str();

    			shared_ptr<SharedBuffer> bufsend(new MemoryBuffer(cstr, bufSize));
    			BufSend(coordId, bufsend, query);
    		}

    		if(myId == coordId)
    		{
    			for(InstanceID i = 0; i<numInstances; ++i)
    			{
    				if (i == myId)
    				{
    					continue;
    				}
    				buf = BufReceive(i, query);
    				std::string bufstring((const char *)buf->getConstData(), buf->getSize());
    				std::stringstream ssout;
    				ssout << bufstring;
    				boost::archive::binary_iarchive ia(ssout);
    				std::vector<SummaryTuple> newlist;
    				ia >> newlist;

    				std::vector<SummaryTuple>::size_type sz = newlist.size();
    				//TODO:assert that the coord vector and slave vectors are equal in size or error out.
    				SummaryTuple summary;
    				for (unsigned att=0; att<sz; att++)
    				{
    					tuples[att].attName = newlist[att].attName;
    					tuples[att].totalCount += newlist[att].totalCount;
    					tuples[att].totalBytes += newlist[att].totalBytes;
    					tuples[att].numChunks += newlist[att].numChunks;

    					if(tuples[att].maxChunkCount < newlist[att].maxChunkCount)
    						tuples[att].maxChunkCount = newlist[att].maxChunkCount;
    					if(tuples[att].minChunkCount > newlist[att].minChunkCount)
    						tuples[att].minChunkCount = newlist[att].minChunkCount;

    					if(tuples[att].minChunkBytes > newlist[att].minChunkBytes)
    						tuples[att].minChunkBytes = newlist[att].minChunkBytes;
    					if( tuples[att].maxChunkBytes < newlist[att].maxChunkBytes)
    						tuples[att].maxChunkBytes = newlist[att].maxChunkBytes;
    				}
    			}
    		}
    	}
        //aggregate the attributes per instance values. This didn't need to be SG'ed
    	//It is stored in aggSummary.
    	if(perIns)
    	{
    		std::vector<SummaryTuple>::size_type sz = tuples.size();
    		SummaryTuple summary;
    		for (unsigned att=0; att<sz; att++)
    		{
    			summary.attName = "";
    			summary.totalCount += tuples[att].totalCount;
    			summary.totalBytes += tuples[att].totalBytes;
    			summary.numChunks  += tuples[att].numChunks;

    			if(summary.maxChunkCount < tuples[att].maxChunkCount)
    				summary.maxChunkCount = tuples[att].maxChunkCount;
    			if(summary.minChunkCount > tuples[att].minChunkCount)
    				summary.minChunkCount = tuples[att].minChunkCount;

    			if(summary.minChunkBytes > tuples[att].minChunkBytes)
    				summary.minChunkBytes = tuples[att].minChunkBytes;
    			if(summary.maxChunkBytes < tuples[att].maxChunkBytes)
    				summary.maxChunkBytes = tuples[att].maxChunkBytes;
    		}
    		aggSummary.push_back(summary);
    	}
    	return true;
    }

    shared_ptr<Array> toArray(Settings const& settings,ArrayDesc const& schema, shared_ptr<Query>& query)
    {
    	shared_ptr<Array> outputArray(new MemArray(schema, query));
    	InstanceID const myId    = query->getInstanceID();
    	InstanceID const coordId = 0;
    	Coordinates position(2,0);
    	size_t numInstances    = query->getInstancesCount();
    	position[0]=myInstanceId;
        //TODO: This pointer code could be accomplished better
    	vector<SummaryTuple> *sp;
    	vector <SummaryTuple>::iterator sumPointer;

    	if (settings.perAttributeflag() && myId!=coordId)
    	{
    		return outputArray;
    	}
    	else if(settings.perAttributeflag() && myId==coordId )
    	{
    		sp = &tuples;
    		sumPointer = tuples.begin();
    	}
    	else if(settings.perInstanceflag())
    	{
    		sp = &aggSummary;
    		sumPointer = aggSummary.begin();
    	}
    	else if(settings.perAttributeflag() == false && settings.perInstanceflag() == false )
    	{
    		sp = &tuples;
    		sumPointer = tuples.begin();
    	}

    	vector<shared_ptr<ArrayIterator> > oaiters(Settings::NUM_OUTPUT_ATTRIBUTES);
    	vector<shared_ptr<ChunkIterator> > ociters(Settings::NUM_OUTPUT_ATTRIBUTES);
    	for(size_t oatt = 0; oatt<Settings::NUM_OUTPUT_ATTRIBUTES; ++oatt)
    	{
    		oaiters[oatt] = outputArray->getIterator(oatt);
    		ociters[oatt] = oaiters[oatt]->newChunk(position).getIterator(query, oatt == 0 ?
    				ChunkIterator::SEQUENTIAL_WRITE :
					ChunkIterator::NO_EMPTY_CHECK | ChunkIterator::SEQUENTIAL_WRITE);
    	}
    	Value buf;

    	for(size_t i=0; i<sp->size(); ++i)
    	{
    		SummaryTuple &t = sumPointer[i];
    		buf.setString(t.attName);
    		ociters[0]->setPosition(position);
    		ociters[0]->writeItem(buf);
    		buf.reset<uint64_t>(t.totalCount);
    		ociters[1]->setPosition(position);
    		ociters[1]->writeItem(buf);
    		buf.setUint64(t.totalBytes);
    		ociters[2]->setPosition(position);
    		ociters[2]->writeItem(buf);
    		buf.setUint64(t.numChunks);
    		ociters[3]->setPosition(position);
    		ociters[3]->writeItem(buf);
    		if(t.numChunks == 0)
    		{
    			buf.setNull();
    		}
    		else
    		{
    			buf.setUint64(t.minChunkCount);
    		}
    		ociters[4]->setPosition(position);
    		ociters[4]->writeItem(buf);
    		if(t.numChunks == 0)
    		{
    			buf.setNull();
    		}
    		else
    		{
    			buf.setDouble(t.totalCount * 1.0 / t.numChunks);
    		}
    		ociters[5]->setPosition(position);
    		ociters[5]->writeItem(buf);
    		if(t.numChunks == 0)
    		{
    			buf.setNull();
    		}
    		else
    		{
    			buf.setUint64(t.maxChunkCount);
    		}
    		ociters[6]->setPosition(position);
    		ociters[6]->writeItem(buf);
    		if(t.numChunks == 0)
    		{
    			buf.setNull();
    		}
    		else
    		{
    			buf.setUint64(t.minChunkBytes);
    		}
    		ociters[7]->setPosition(position);
    		ociters[7]->writeItem(buf);
    		if(t.numChunks == 0)
    		{
    			buf.setNull();
    		}
    		else
    		{
    			buf.setDouble(t.totalBytes * 1.0 / t.numChunks);
    		}
    		ociters[8]->setPosition(position);
    		ociters[8]->writeItem(buf);
    		if(t.numChunks == 0)
    		{
    			buf.setNull();
    		}
    		else
    		{
    			buf.setUint64(t.maxChunkBytes);
    		}
    		ociters[9]->setPosition(position);
    		ociters[9]->writeItem(buf);
    		position[1]++;
    	}
    	for(size_t oatt = 0; oatt<Settings::NUM_OUTPUT_ATTRIBUTES; ++oatt)
    	{
    		ociters[oatt]->flush();
    	}

    	return outputArray;
    }
};

} } //namespaces

#endif //summarize_settings
