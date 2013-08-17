package cosmos.util.sql;

import java.util.Set;

import cosmos.options.Index;

public interface SchemaDefiner<T> {

	public String getDataTable();
	
	public Set<Index> getIndexColumns();

	public AccumuloIterables<T> iterator(SelectQuery query);
}
