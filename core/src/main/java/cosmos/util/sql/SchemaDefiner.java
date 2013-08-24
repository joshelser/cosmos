package cosmos.util.sql;

import java.util.List;
import java.util.Set;

import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import cosmos.options.Index;
import cosmos.util.sql.AccumuloRel.Plan;

/**
 * Interface that defines an interface that enables us funcionality in defining
 * our referential schema.
 * 
 * @author phrocker
 * 
 * @param <T>
 */

public interface SchemaDefiner<T> {

	public void register(AccumuloSchema<?> parentSchema);

	public String getDataTable();

	public Set<Index> getIndexColumns(String table);

	public AccumuloIterables<T> iterator(List<String> schemaLayout,
			AccumuloRel.Plan query, Plan aggregatePlan);
}
