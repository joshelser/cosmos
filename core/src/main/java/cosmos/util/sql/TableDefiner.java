package cosmos.util.sql;

/**
 * Interface that defines an interface that enables us funcionality in defining
 * our referential schema.
 * 
 * @author phrocker
 * 
 * @param <T>
 */

public interface TableDefiner {

	public AccumuloTable<?> getTable(String name);
}
