package cosmos.util.sql;

import java.util.Set;

import cosmos.options.Index;

/**
 * Interface that defines an interface that enables us
 * funcionality in defining our referential schema.
 * @author phrocker
 *
 * @param <T>
 */
public interface SchemaDefiner<T> {

	public String getDataTable();
	
	public Set<Index> getIndexColumns();

	public AccumuloIterables<T> iterator(SelectQuery query);
}
