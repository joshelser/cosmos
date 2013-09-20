package cosmos.sql;

import java.util.Collection;
import java.util.Collections;

import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.java.MapSchema;

import org.apache.log4j.Logger;

public class AccumuloSchema<T extends SchemaDefiner<?>> extends MapSchema {
  
  protected T meataData;
  
  private Class<? extends AccumuloTable<?>> clazz = null;
  
  private static final Logger log = Logger.getLogger(AccumuloSchema.class);
  
  /**
   * Accumulo schema constructor
   */
  public AccumuloSchema(Schema parentSchema, String name, String host, String database, Expression expression, T schemaDefiner,
      Class<? extends AccumuloTable<?>> clazz) {
    super(parentSchema, name, expression);
    meataData = schemaDefiner;
    // let's maek a cyclic dependency
    meataData.register(this);
    this.clazz = clazz;
  }
  
  /**
   * Returns the table associated with the class
   */
  public AccumuloTable<?> getTable(String name) {
    
    if (meataData instanceof TableDefiner) {
      return ((TableDefiner) meataData).getTable(name);
    } else
      return (AccumuloTable<?>) tableMap.get(name).getTable(Class.class);
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public <E> Table<E> getTable(String name, Class<E> elementType) {
    return (Table<E>) getTable(name);
  }
  
  @Override
  protected Collection<TableInSchema> initialTables() {
    return Collections.EMPTY_LIST;
    
  }
  
}
