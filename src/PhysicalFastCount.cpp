
/**
 * @file PhysicalFastCount.cpp
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

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.fast_count"));

using namespace scidb;

static void EXCEPTION_ASSERT(bool cond)
{
	if (! cond)
	{
		throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
	}
}

class PhysicalFastCount : public PhysicalOperator
{
public:
	PhysicalFastCount(std::string const& logicalName,
			std::string const& physicalName,
			Parameters const& parameters,
			ArrayDesc const& schema):
				PhysicalOperator(logicalName, physicalName, parameters, schema)
{}

	virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
	{
		return true;
	}

#ifdef CPP11
virtual RedistributeContext getOutputDistribution(std::vector<RedistributeContext> const&, std::vector<ArrayDesc> const&) const
{
	return RedistributeContext(psUndefined);
}
#else
virtual ArrayDistribution getOutputDistribution(std::vector<ArrayDistribution> const&, std::vector<ArrayDesc> const&) const
{
	return ArrayDistribution(psUndefined);
}
#endif


size_t exchangeCount(size_t instancecount, shared_ptr<Query>& query)
{

	InstanceID const myId    = query->getInstanceID();
	InstanceID const coordId = 0;

	size_t const numInstances = query->getInstancesCount();
	size_t const scalarSize   = sizeof(size_t);


	shared_ptr<SharedBuffer> bufsend(new MemoryBuffer( &(instancecount), scalarSize));

	size_t tempcount;
	shared_ptr<SharedBuffer> buf(new MemoryBuffer( &(tempcount), scalarSize));

	size_t count = instancecount;

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

			size_t tempcount;
			memcpy(&tempcount, buf->getData(), scalarSize);

			count = count + tempcount;
		}
	}


	return count;
}


std::shared_ptr< Array> execute(std::vector< std::shared_ptr< Array> >& inputArrays, std::shared_ptr<Query> query)
    		{
	//FastCountSettings settings (_parameters, false, query);

	shared_ptr<Array>& input = inputArrays[0];
	shared_ptr< Array> outArray;

	std::shared_ptr<ConstArrayIterator> inputIterator = input->getConstIterator(0);
	size_t count = 0;

	while(!inputIterator-> end())
	{

		std::shared_ptr<ConstChunkIterator> inputChunkIterator = inputIterator->getChunk().getConstIterator();

		ConstChunk const& chunk = inputChunkIterator->getChunk();
		count+= chunk.count();

		++(*inputIterator);
	}

	size_t remotecount =  exchangeCount(count, query);
	shared_ptr<Array> outputArray(new MemArray(_schema, query));

	if(query->getInstanceID() == 0)
	{

		shared_ptr<ArrayIterator> outputArrayIter = outputArray->getIterator(0);
		Coordinates position(1,0);

		shared_ptr<ChunkIterator> outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
		outputChunkIter->setPosition(position);

		Value value;
		value.setUint64(remotecount);
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

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalFastCount, "fast_count", "PhysicalFastCount");


} // end namespace scidb
