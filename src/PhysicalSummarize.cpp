
/**
 * @file PhysicalSummarize.cpp
 *
 * @brief count using the chunk map instead of iteration over all chunks
 *
 * @author Jonathan Rivers <jrivers96@gmail.com>
 * @author others
 */

#include <limits>
#include <sstream>
#include <memory>
#include <string>
#include <vector>
#include <ctype.h>

#include <system/Exceptions.h>
#include <system/SystemCatalog.h>
#include <system/Sysinfo.h>

#include <query/TypeSystem.h>
#include <query/FunctionDescription.h>
#include <query/FunctionLibrary.h>
#include <query/Operator.h>
#include <query/TypeSystem.h>

#include <array/DBArray.h>
#include <array/Tile.h>
#include <array/TileIteratorAdaptors.h>

#include <util/Platform.h>
#include <util/Network.h>

#include <boost/algorithm/string.hpp>
#include <boost/unordered_map.hpp>
#include <boost/lexical_cast.hpp>

#include <fcntl.h>

#include <log4cxx/logger.h>


#ifdef CPP11
using std::shared_ptr;
using std::make_shared;
#else
using boost::shared_ptr;
using boost::make_shared;
#endif

using boost::algorithm::trim;
using boost::starts_with;
using boost::lexical_cast;
using boost::bad_lexical_cast;

using namespace std;

using boost::algorithm::is_from_range;


namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.summarize"));

using namespace scidb;

static void EXCEPTION_ASSERT(bool cond)
{
	if (! cond)
	{
		throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
	}
}

class PhysicalSummarize : public PhysicalOperator
{
public:
	PhysicalSummarize(std::string const& logicalName,
			std::string const& physicalName,
			Parameters const& parameters,
			ArrayDesc const& schema):
				PhysicalOperator(logicalName, physicalName, parameters, schema)
{}

   virtual RedistributeContext getOutputDistribution(std::vector<RedistributeContext> const&,
	                                                      std::vector<ArrayDesc> const&) const
    {
	   return RedistributeContext(_schema.getDistribution(),
	                              _schema.getResidency());
    }

	virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
	{
		return true;
	}

struct pack {
	  size_t min;
	  size_t max;
	  size_t count;
	};

struct summary {
		  size_t min;
		  size_t max;
		  size_t count;
		  double avg;
	} ;


summary exchangeVals(size_t instancemin, size_t instancemax, size_t instancecount, shared_ptr<Query>& query)
{

	InstanceID const myId    = query->getInstanceID();
	InstanceID const coordId = 0;

	size_t const numInstances = query->getInstancesCount();

    pack sendbuf;
    pack tempbuf;

	sendbuf.count = instancecount;
	sendbuf.min   = instancemin;
	sendbuf.max   = instancemax;


	size_t const scalarSize   = sizeof(size_t);
	size_t const bufSize      = sizeof(sendbuf);

	shared_ptr<SharedBuffer> bufsend(new MemoryBuffer( &(sendbuf), bufSize));

	shared_ptr<SharedBuffer> buf(new MemoryBuffer( &(tempbuf), bufSize));

	summary output;
	output.count = instancecount;
	output.max = 0;
	output.min = SIZE_MAX;
    output.avg = 0.0;

    if(myId != coordId)
	{
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

			memcpy(&tempbuf, buf->getData(), bufSize);

			output.count = output.count + tempbuf.count;

			if(output.min > tempbuf.count )
		        output.min = tempbuf.count;

			if(output.max < tempbuf.count )
		         output.max = tempbuf.count;
		}

		output.avg = (double)output.count/(double)numInstances;

	}


	return output;
}


std::shared_ptr< Array> execute(std::vector< std::shared_ptr< Array> >& inputArrays, std::shared_ptr<Query> query)
    		{
	//summarizeSettings settings (_parameters, false, query);

	shared_ptr<Array>& input = inputArrays[0];
	shared_ptr< Array> outArray;

	std::shared_ptr<ConstArrayIterator> inputIterator = input->getConstIterator(0);

	size_t count = 0;
	size_t min = SIZE_MAX;
	size_t max = 0;
	double average = 0;
    size_t temp;

	while(!inputIterator-> end())
	{

		ConstChunk const& chunk = inputIterator->getChunk();
	    temp = chunk.count();
		count+= temp;

		if(min > temp )
        	min = temp;

		if(max < temp )
            max = temp;

        //chunk counts min max and average
		//usize, csize, asize

		++(*inputIterator);
	}

	stringstream ss;
	ss << "SUMMARIZEDEBUG, preExchange: min():" << min << ", max():" << max << ",count():" << count;
	LOG4CXX_DEBUG (logger, ss.str());

	summary remoteval =  exchangeVals(min,max,count,query);

	ss << "SUMMARIZEDEBUG, postExchange: min():" << remoteval.min << ", max():" << remoteval.max << ",count():" << remoteval.count;
	LOG4CXX_DEBUG (logger, ss.str());

	shared_ptr<Array> outputArray(new MemArray(_schema, query));

	if(query->getInstanceID() == 0)
	{
		Value value;

		shared_ptr<ArrayIterator> outputArrayIter = outputArray->getIterator(0);
		Coordinates position(1,0);
		shared_ptr<ChunkIterator> outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
		outputChunkIter->setPosition(position);

		value.setUint64(remoteval.count);
		outputChunkIter->writeItem(value);
		outputChunkIter->flush();

		outputArrayIter = outputArray->getIterator(1);
		outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
		outputChunkIter->setPosition(position);

		value.setUint64(remoteval.min);
		outputChunkIter->writeItem(value);
		outputChunkIter->flush();

		outputArrayIter = outputArray->getIterator(2);
		outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
		outputChunkIter->setPosition(position);

		value.setUint64(remoteval.max);
		outputChunkIter->writeItem(value);
		outputChunkIter->flush();

		outputArrayIter = outputArray->getIterator(3);
		outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
		outputChunkIter->setPosition(position);

		value.setDouble(remoteval.avg);
		outputChunkIter->writeItem(value);
		outputChunkIter->flush();

		return outputArray;

	}

	else
	{
		return outputArray;
	}


   }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalSummarize, "summarize", "PhysicalSummarize");


} // end namespace scidb
