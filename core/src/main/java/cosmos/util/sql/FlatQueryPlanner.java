package cosmos.util.sql;

import cosmos.util.sql.AccumuloRel.Planner;
import cosmos.util.sql.call.Field;
import cosmos.util.sql.call.Fields;
import cosmos.util.sql.call.impl.Filter;
import cosmos.util.sql.call.impl.Projection;

/**
 * @TODO: remove this ugly class
 * @author phrocker
 *
 */
public class FlatQueryPlanner extends Planner {
	
	
	public Projection getProjection()
	{
		return (Projection) operations.get(IMPLEMENTOR_TYPE.PROJECTION);
	}
	
	public Filter getFilter()
	{
		return (Filter) operations.get(IMPLEMENTOR_TYPE.FILTER);
	}
	
	public Fields getLimitFields()
	{
		return (Fields) operations.get(IMPLEMENTOR_TYPE.FIELD_SELECT);
	}

}
