
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <query/Operator.h>
#include <log4cxx/logger.h>

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


//#include "summarizeSettings.h"

namespace scidb
{

class LogicalSummarize : public  LogicalOperator
{
public:
	LogicalSummarize(const std::string& logicalName, const std::string& alias):
		LogicalOperator(logicalName, alias)
{
		ADD_PARAM_INPUT();
		//ADD_PARAM_VARIES();
}

	ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, shared_ptr< Query> query)
	{
		ArrayDesc const& inputSchema = schemas[0];
		//summarizeSettings settings (_parameters, true, query);
		vector<DimensionDesc> dimensions(1);
		size_t const nInstances = query->getInstancesCount();
		dimensions[0] = DimensionDesc("i",    0, 0, CoordinateBounds::getMax(), CoordinateBounds::getMax(), 1, 0);
		// dimensions[0] = DimensionDesc("i", 0, 0, nInstances-1, nInstances-1, 1, 0);
		vector<AttributeDesc> attributes;
		attributes.push_back(AttributeDesc((AttributeID)0, "count", TID_UINT64, AttributeDesc::IS_NULLABLE, 0));
		return ArrayDesc("summarize", attributes, dimensions, defaultPartitioning());
	}
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalSummarize, "summarize");

} // emd namespace scidb
