
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

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
// Provide an implementation of serialize for std::list
#include <boost/serialization/list.hpp>
#include <boost/serialization/vector.hpp>

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
private:
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive & ar, const unsigned int /*version*/)
  {
    ar & attid;
    ar & min;
    ar & max;
    ar & count;
    ar & numchunks;
  }
public:

	  long attid;
	  size_t min;
	  size_t max;
	  size_t count;
      size_t numchunks;
      double avg;
};

struct summary {
		  std::vector<long> attid;
	      std::vector<size_t> min;
		  std::vector<size_t> max;
		  std::vector<size_t> count;
		  std::vector<double> avg;
	} ;


class packbin
{
private:
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive & ar, const unsigned int /*version*/)
  {
    // This is the only thing you have to implement to serialize a std::list<foo>
    ar & value;
    // if we had more members here just & each of them with ar
  }
public:
  pack value;
};

bool exchangeVals(std::vector<packbin> &instancepack, shared_ptr<Query>& query)
{

	stringstream ss;
	ss << "SUMMARIZEDEBUG, exchangeVals():" << std::endl;
	LOG4CXX_DEBUG (logger, ss.str());

	size_t numAtt = instancepack.size();
	InstanceID const myId    = query->getInstanceID();
	InstanceID const coordId = 0;

	size_t const numInstances = query->getInstancesCount();

	/*
    summary aggstats;

    aggstats.attid.reserve(numAtt);
	aggstats.avg.reserve(numAtt);
	aggstats.count.reserve(numAtt);
	aggstats.max.reserve(numAtt);
	aggstats.min.reserve(numAtt);
    */

	shared_ptr<SharedBuffer> buf;

	if(myId != coordId)
	{
		std::stringstream out;
		// serialize into the stream
		boost::archive::binary_oarchive oa(out);
		oa << instancepack;

		out.seekg(0, ios::end);
		size_t bufSize = out.tellg();
		out.seekg(0,ios::beg);

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

			//ss << "SUMMARIZEDEBUG, bufrecieve: " << std::to_string((int)i) << std::endl;
			//LOG4CXX_DEBUG (logger, ss.str());

			std::string bufstring((const char *)buf->getConstData(), buf->getSize());
			std::stringstream ssout;
			ssout << bufstring;

			boost::archive::binary_iarchive ia(ssout);
			std::vector<packbin> newlist;
			ia >> newlist;

			std::vector<packbin>::size_type sz = newlist.size();
            //TODO:assert that the coord vector and slave vectors are equal in size or error out.

			// assign some values:
			  for (unsigned i=0; i<sz; i++)
			  {
                instancepack[i].value.count += newlist[i].value.count;
                instancepack[i].value.numchunks += newlist[i].value.numchunks;

                if(instancepack[i].value.max < newlist[i].value.max)
                	instancepack[i].value.max = newlist[i].value.max;

                if(instancepack[i].value.min > newlist[i].value.min)
                	instancepack[i].value.min = newlist[i].value.min;

			  }

			//ss << "SUMMARIZEDEBUG, coordreceive: " << std::to_string((int)i) << ",front():attid:" << newlist.front().value.attid << " count:" << std::to_string((int)newlist.front().value.count) <<std::endl;
			//LOG4CXX_DEBUG (logger, ss.str());
            //delete [] token;
			//ssout.clear();

		}

		std::vector<packbin>::size_type sz = instancepack.size();
		for (unsigned i=0; i<sz; i++)
		{
			instancepack[i].value.avg = (double)instancepack[i].value.count/ (double)instancepack[i].value.numchunks;
		}

	}



	return true;
}


std::shared_ptr< Array> execute(std::vector< std::shared_ptr< Array> >& inputArrays, std::shared_ptr<Query> query)
    		{
	//summarizeSettings settings (_parameters, false, query);
	 stringstream ss;
	 ss << "SUMMARIZEDEBUG,Begin execution" << std::endl;
	 LOG4CXX_DEBUG (logger, ss.str());
	 shared_ptr<Array>& inputArray = inputArrays[0];
	shared_ptr< Array> outArray;

	size_t numAtt = inputArray->getArrayDesc().getAttributes().size()-1;
	std::vector<shared_ptr<ConstArrayIterator> > iaiters(numAtt, NULL);
	//vector<shared_ptr<ConstChunkIterator> > iciters(numAtt, NULL);
	//vector<Value const*> inputVal(numAtt, NULL);
    //std::vector<pack>sendList;
    // sendList.reserve(sizeof(pack) * numAtt);
    std::vector<packbin> sendList;
    sendList.reserve(numAtt);

    packbin listPack;
    pack* listPackref =  &(listPack.value);

	iaiters[0] = inputArray->getConstIterator(numAtt);

	ss << "SUMMARIZEDEBUG,for loop" << std::endl;
	LOG4CXX_DEBUG (logger, ss.str());

	for(size_t a=0; a<numAtt; ++a)
        {
            iaiters[a] = inputArray->getConstIterator( a);


    listPackref->count = 0;
	listPackref->min = SIZE_MAX;
	listPackref->max = 0;
	listPackref->numchunks = 0;
    size_t temp;

	while(!iaiters[a]-> end())
	{

		ConstChunk const& chunk = iaiters[a]->getChunk();
	    temp = chunk.count();

	    listPackref->count+= temp;
		listPackref->numchunks += 1;

		if(listPackref->min > temp )
        	listPackref->min = temp;

		if(listPackref->max < temp )
            listPackref->max = temp;

        //chunk counts min max and average
		//usize, csize, asize

		++(*iaiters[a]);
	}
    listPackref->attid = a;
	sendList.push_back(listPack);
   }

	ss << "SUMMARIZEDEBUG, Before exchangevals" << std::endl;
	LOG4CXX_DEBUG (logger, ss.str());

	bool rtnval = exchangeVals(sendList,query);

	//summary remoteval;
	//ss << "SUMMARIZEDEBUG, postExchange: min():" << remoteval.min << ", max():" << remoteval.max << ",count():" << remoteval.count;
	//LOG4CXX_DEBUG (logger, ss.str());

	shared_ptr<Array> outputArray(new MemArray(_schema, query));

	if(query->getInstanceID() == 0)
	{

		for(size_t a=0; a<numAtt; ++a)
		   {

			Value value;

		shared_ptr<ArrayIterator> outputArrayIter = outputArray->getIterator(0);
		Coordinates position(1,0);
		position[0] = a;
		shared_ptr<ChunkIterator> outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
		outputChunkIter->setPosition(position);
		value.setUint64(sendList[a].value.count);
		outputChunkIter->writeItem(value);
		outputChunkIter->flush();

		outputArrayIter = outputArray->getIterator(1);
		outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
		outputChunkIter->setPosition(position);
		value.setUint64(sendList[a].value.min);
		outputChunkIter->writeItem(value);
		outputChunkIter->flush();

		outputArrayIter = outputArray->getIterator(2);
		outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
		outputChunkIter->setPosition(position);
		value.setUint64(sendList[a].value.max);
		outputChunkIter->writeItem(value);
		outputChunkIter->flush();

		outputArrayIter = outputArray->getIterator(3);
		outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
		outputChunkIter->setPosition(position);
		value.setDouble(sendList[a].value.avg);
		outputChunkIter->writeItem(value);
		outputChunkIter->flush();
	}
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
