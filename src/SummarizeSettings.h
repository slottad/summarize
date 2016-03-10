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
* along with accelerated_io_tools.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#ifndef GROUPED_AGGREGATE_SETTINGS
#define GROUPED_AGGREGATE_SETTINGS

#include <query/Operator.h>
#include <query/AttributeComparator.h>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

namespace scidb
{
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
 * Settings for the grouped_aggregate operator.
 */
class Settings
{
private:
    size_t _numInputAttributes;
    size_t _numInstances;

public:
    Settings(ArrayDesc const& inputSchema,
             vector< shared_ptr<OperatorParam> > const& operatorParameters,
             bool logical,
             shared_ptr<Query>& query):
        _numInputAttributes(inputSchema.getAttributes().size()),
        _numInstances(query->getInstancesCount())
    {}

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

    size_t numInstances() const
    {
        return _numInstances;
    }
};

struct SummaryTuple
{
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

    shared_ptr<Array> toArray(ArrayDesc const& schema, shared_ptr<Query>& query)
    {
        shared_ptr<Array> outputArray(new MemArray(schema, query));
        Coordinates position(2,0);
        position[0]=myInstanceId;
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
        for(size_t i=0; i<tuples.size(); ++i)
        {
            SummaryTuple& t = tuples[i];
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

#endif //grouped_aggregate_settings
