
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


//#include "FastCountSettings.h"

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

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        ArrayDesc const& inputSchema = schemas[0];
        //FastCountSettings settings (_parameters, true, query);
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
