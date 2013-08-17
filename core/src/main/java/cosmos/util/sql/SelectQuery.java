package cosmos.util.sql;

import cosmos.util.sql.AccumuloRel.Implementor;
import cosmos.util.sql.call.Field;
import cosmos.util.sql.call.impl.Filter;
import cosmos.util.sql.call.impl.Projection;

public class SelectQuery extends Implementor {
	
	
	public Projection getProjection()
	{
		return (Projection) operations.get(IMPLEMENTOR_TYPE.PROJECTION);
	}
	
	public Filter getFilter()
	{
		return (Filter) operations.get(IMPLEMENTOR_TYPE.FILTER);
	}
	
	public Field getLimitFields()
	{
		return (Field) operations.get(IMPLEMENTOR_TYPE.SELECT);
	}

}
