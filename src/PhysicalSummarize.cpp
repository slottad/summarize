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

#include <limits>
#include <sstream>
#include <memory>
#include <string>
#include <vector>
#include <ctype.h>
#include <query/TypeSystem.h>
#include <query/Operator.h>
#include <log4cxx/logger.h>
#include <smgr/io/InternalStorage.h>
#include "SummarizeSettings.h"

using std::shared_ptr;
using std::make_shared;
using namespace std;

namespace scidb
{

using namespace scidb;

class PhysicalSummarize : public PhysicalOperator
{
public:
	PhysicalSummarize(std::string const& logicalName,
        std::string const& physicalName,
        Parameters const& parameters,
        ArrayDesc const& schema):
            PhysicalOperator(logicalName, physicalName, parameters, schema)
{}

 virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
 {
     return true;
 }

 virtual RedistributeContext getOutputDistribution(
            std::vector<RedistributeContext> const& inputDistributions,
            std::vector< ArrayDesc> const& inputSchemas) const
 {
     return RedistributeContext(createDistribution(psUndefined), _schema.getResidency() );
 }


    struct Descriptinator
    {
        enum class ST { uncompressed, compressed, allocated };
            
        Descriptinator(summarize::InstanceSummary& summary, const ArrayID& aid, string size_type)
            : _summary(summary), _aid(aid), _st(ST::uncompressed)
            {
                if (size_type == "uncompressed" or size_type == "usize")
                    _st = ST::uncompressed;
                else if (size_type == "compressed" or size_type == "csize")
                    _st = ST::compressed;
                else if (size_type == "allocated" or size_type == "asize")
                    _st = ST::allocated;
                // Should probably throw an error if the size_type is
                // unrecognized, but shouldn't that be caught much
                // earlier?
            }

        void list(const ChunkDescriptor& desc, bool free) 
            {
                if (_aid == desc.hdr.arrId) {
                    uint64_t size = 0;
                    switch (_st) {
                    case ST::uncompressed: size = desc.hdr.size;
                        break;
                    case ST::compressed: size = desc.hdr.compressedSize;
                        break;
                    case ST::allocated: size = desc.hdr.allocatedSize;
                        break;
                    }
                    _summary.addChunkData(desc.hdr.attId, size, desc.hdr.nElems);
                }
            }
        
    private:
        summarize::InstanceSummary& _summary;
        const ArrayID& _aid;
        ST _st;
    };
    
    
std::shared_ptr< Array> execute(std::vector< std::shared_ptr< Array> >& inputArrays, std::shared_ptr<Query> query)
{
    shared_ptr<Array>& inputArray = inputArrays[0];
    ArrayDesc const& inputSchema = inputArray->getArrayDesc();
    summarize::Settings settings(inputSchema, _parameters, false, query);
    size_t const numInputAtts= settings.numInputAttributes();
    vector<string> attNames(numInputAtts);
    vector<shared_ptr<ConstArrayIterator> > iaiters(numInputAtts);
    for(AttributeID i =0; i<numInputAtts; ++i)
    {
        attNames[i] = inputSchema.getAttributes()[i].getName();
        iaiters[i] = inputArray->getConstIterator(i);
    }
    summarize::InstanceSummary summary(query->getInstanceID(), numInputAtts, attNames);

    if (settings.useSizeType()) {
        ArrayID id;
        if (!iaiters[0]->end()) {
            // AFAIK, the easiest way to get the versioned array id
            // from the unversioned name is to Materialize the first
            // chunk and use its ArrayDesc. If there is a more correct way...
            ConstChunk const& chunk = iaiters[0]->getChunk();
            id = chunk.getArrayDesc().getId();
        } else {
            id  = inputSchema.getId();
        }
        
        Descriptinator desc(summary, id, settings.getSizeType());
        StorageManager::getInstance().visitChunkDescriptors(
            Storage::ChunkDescriptorVisitor(
                boost::bind(
                        &Descriptinator::list,&desc,_1,_2)));
    } else {
        for(AttributeID i=0; i<numInputAtts; ++i)
            {
                while(!iaiters[i]->end())
                    {
                        ConstChunk const& chunk = iaiters[i]->getChunk();
                        
                        summary.addChunkData(i, chunk.getSize(), chunk.count());
                        ++(*iaiters[i]);
                    }
            }
    }
    summary.makeFinalSummary(settings, _schema, query);
    return summary.toArray(settings, _schema, query);
}
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalSummarize, "summarize", "PhysicalSummarize");


} // end namespace scidb
