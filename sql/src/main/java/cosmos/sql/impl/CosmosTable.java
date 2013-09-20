package cosmos.sql.impl;

import java.lang.reflect.Type;
import java.util.List;

import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Linq4j;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptTable.ToRelContext;
import org.eigenbase.reltype.RelDataType;

import com.google.common.base.Preconditions;

import cosmos.results.impl.MultimapQueryResult;
import cosmos.sql.AccumuloIterables;
import cosmos.sql.AccumuloRel;
import cosmos.sql.AccumuloRel.Plan;
import cosmos.sql.AccumuloTable;
import cosmos.sql.CosmosSchema;
import cosmos.sql.SchemaDefiner;
import cosmos.sql.TableScanner;

/**
 * Cosmos table representation.
 * 
 * @author phrocker
 * 
 */
public class CosmosTable extends AccumuloTable<Object[]> {
  
  protected JavaTypeFactory javaFactory;
  
  protected RelDataType rowType;
  
  private SchemaDefiner<?> metadata;
  
  protected String table;
  
  public CosmosTable(CosmosSchema<? extends SchemaDefiner<?>> meataSchema, SchemaDefiner<?> metadata, JavaTypeFactory typeFactory, RelDataType rowType,
      String table) {
    
    super(meataSchema, table, typeFactory);
    javaFactory = typeFactory;
    
    this.metadata = metadata;
    
    this.rowType = rowType;
    
    this.table = table;
    
  }
  
  @Override
  public RelDataType getRowType() {
    
    return rowType;
  }
  
  @Override
  public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
    
    return new TableScanner(context.getCluster(), context.getCluster().traitSetOf(AccumuloRel.CONVENTION), relOptTable, this, relOptTable.getRowType()
        .getFieldNames());
  }
  
  @SuppressWarnings("unchecked")
  public Enumerable<Object[]> accumulate(List<String> fieldNames) {
    Plan query = plans.poll();
    
    Plan aggregatePlan = aggregationPlans.poll();
    
    Preconditions.checkNotNull(query);
    
    resultSet = (AccumuloIterables<Object[]>) metadata.iterator(fieldNames, query, aggregatePlan);
    
    return Linq4j.asEnumerable(resultSet);
  }
  
  @Override
  public Type getElementType() {
    return MultimapQueryResult.class;
  }
  
  public String getTable() {
    return table;
  }
  
}
