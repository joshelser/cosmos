package cosmos.sql.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.log4j.Logger;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeFactory.FieldInfoBuilder;

import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import cosmos.Cosmos;
import cosmos.UnexpectedStateException;
import cosmos.UnindexedColumnException;
import cosmos.impl.SortableResult;
import cosmos.options.Index;
import cosmos.results.Column;
import cosmos.results.SValue;
import cosmos.results.impl.MultimapQueryResult;
import cosmos.sql.AccumuloIterables;
import cosmos.sql.AccumuloRel;
import cosmos.sql.AccumuloSchema;
import cosmos.sql.AccumuloTable;
import cosmos.sql.ResultDefiner;
import cosmos.sql.TableDefiner;
import cosmos.sql.call.CallIfc;
import cosmos.sql.call.BaseVisitor;
import cosmos.sql.call.Field;
import cosmos.sql.call.Fields;
import cosmos.sql.call.ChildVisitor;
import cosmos.sql.call.impl.Filter;
import cosmos.sql.impl.functions.FieldLimiter;

/**
 * @TODO gut this class. it needs a rework.
 * @author marc
 * 
 */
public class CosmosSql extends ResultDefiner implements TableDefiner {
  
  /**
   * Main cosmos reference
   */
  protected Cosmos cosmos;
  
  protected Iterable<MultimapQueryResult> iter;
  
  protected Collection<Iterable<MultimapQueryResult>> plannedParentHood;
  
  /**
   * Iterator reference
   */
  protected Iterator<MultimapQueryResult> baseIter;
  
  private JavaTypeFactory typeFactory;
  
  private AccumuloSchema<?> schema;
  
  private static final Logger log = Logger.getLogger(CosmosSql.class);
  
  protected Cache<String,CosmosTable> tableCache = CacheBuilder.newBuilder().expireAfterAccess(24, TimeUnit.HOURS).build();
  
  /**
   * Constructor
   */
  
  public CosmosSql(Cosmos cosmosImpl) throws MutationsRejectedException, TableNotFoundException, UnexpectedStateException {
    plannedParentHood = Lists.newArrayList();
    iter = Collections.emptyList();
    this.cosmos = cosmosImpl;
    
  }
  
  @Override
  public String getDataTable() {
    return "";
    
  }
  
  @Override
  public AccumuloIterables<Object[]> iterator(List<String> schemaLayout, AccumuloRel.Plan planner, AccumuloRel.Plan aggregatePlan) {
    
    plannedParentHood = Lists.newArrayList();
    iter = Collections.emptyList();
    Iterator<Object[]> returnIter = Iterators.emptyIterator();
    
    BaseVisitor<? extends CallIfc<?>> query = planner.getChildren();
    
    Collection<? extends CallIfc<?>> filters = query.children(Filter.class.getSimpleName());
    
    Collection<? extends CallIfc<?>> fields = query.children("selectedFields");
    
    String table = ((CosmosTable) planner.table).getTable();
    
    SortableResult res;
    try {
      
      res = cosmos.fetch(table);
      if (aggregatePlan == null) {
        
        if (res != null) {
          if (filters.size() > 0) {
            for (CallIfc<?> filterIfc : filters) {
              Filter filter = (Filter) filterIfc;
              
              for (ChildVisitor subFilter : filter.getFilters()) {
                plannedParentHood.add(buildFilterIterator(filter.getFilters(), res));
                
              }
            }
          } else {
            plannedParentHood.add(cosmos.fetch(res));
          }
          
          for (Iterable<MultimapQueryResult> subIter : plannedParentHood) {
            if (iter == null) {
              iter = subIter;
            } else {
              iter = Iterables.concat(iter, subIter);
            }
          }
          
          List<Field> fieldsUserWants = Lists.newArrayList();
          
          for (CallIfc<?> fiel : fields) {
            Fields fieldList = (Fields) fiel;
            fieldsUserWants.addAll(fieldList.getFields());
          }
          
          baseIter = iter.iterator();
          baseIter = Iterators.transform(baseIter, new FieldLimiter(fieldsUserWants));
          
          returnIter = Iterators.transform(baseIter, new DocumentExpansion(schemaLayout));
        }
      } else {
        BaseVisitor<? extends CallIfc<?>> aggregates = aggregatePlan.getChildren();
        Collection<Field> groupByFields = (Collection<Field>) aggregates.children("groupBy");
        Iterator<Field> fieldIter = groupByFields.iterator();
        while (fieldIter.hasNext()) {
          
          String groupByField = fieldIter.next().toString();
          returnIter = Iterators.transform(cosmos.groupResults(res, new Column(groupByField)).iterator(), new GroupByResultFuckit());
        }
        
      }
      
    } catch (UnexpectedStateException e1) {
      log.error(e1);
    } catch (TableNotFoundException e) {
      log.error(e);
    } catch (UnindexedColumnException e) {
      log.error(e);
    }
    
    return new AccumuloIterables<Object[]>(returnIter);
  }
  
  private Iterable<MultimapQueryResult> buildFilterIterator(List<ChildVisitor> filters, SortableResult res) {
    
    Iterable<MultimapQueryResult> baseIter = Collections.emptyList();
    System.out.println("filters size is " + filters.size());
    Iterable<Iterable<MultimapQueryResult>> ret = Iterables.transform(filters, new LogicVisitor(cosmos, res));
    for (Iterable<MultimapQueryResult> iterable : ret) {
      baseIter = Iterables.concat(baseIter, iterable);
    }
    
    return baseIter;
    
  }
  
  class DocumentExpansion implements Function<MultimapQueryResult,Object[]> {
    
    private List<String> fields;
    
    public DocumentExpansion(List<String> fields) {
      this.fields = fields;
    }
    
    public Object[] apply(MultimapQueryResult document) {
      
      Object[] results = new List[fields.size()];
      
      for (int i = 0; i < fields.size(); i++) {
        String field = fields.get(i);
        Column col = new Column(field);
        Collection<SValue> values = document.get(col);
        if (values != null) {
          
          List<Entry<Column,SValue>> columns = Lists.newArrayList();
          for (SValue value : values) {
            columns.add(Maps.immutableEntry(col, value));
          }
          results[i] = columns;// values.iterator().next().value();
          
        } else {
          results[i] = new ArrayList<SValue>();
        }
      }
      return results;
      
    }
    
  }
  
  class GroupByResultFuckit implements Function<Entry<SValue,Long>,Object[]> {
    
    public GroupByResultFuckit() {}
    
    public Object[] apply(Entry<SValue,Long> result) {
      
      Object[] results = new Object[2];
      results[0] = result.getKey();
      results[1] = result.getValue();
      
      return results;
      
    }
    
  }
  
  @Override
  public AccumuloTable<?> getTable(String name) {
    CosmosTable table = tableCache.getIfPresent(name);
    try {
      
      if (table == null) {
        SortableResult sort = cosmos.fetch(name);
        
        FieldInfoBuilder builder = new RelDataTypeFactory.FieldInfoBuilder();
        
        for (Index indexField : sort.columnsToIndex()) {
          builder.add(indexField.column().name(), typeFactory.createType(indexField.getIndexTyped()));
        }
        
        final RelDataType rowType = typeFactory.createStructType(builder);
        
        try {
          table = new CosmosTable(schema, this, typeFactory, rowType, name);
          
          tableCache.put(name, table);
        } catch (SecurityException e) {
          log.error(e);
        }
      }
      
    } catch (UnexpectedStateException e) {
      log.error(e);
      
    }
    
    return table;
  }
  
  @Override
  public Set<Index> getIndexColumns(String table) {
    SortableResult sort;
    try {
      sort = cosmos.fetch(table);
      
      if (sort != null) {
        return sort.columnsToIndex();
      }
      
    } catch (UnexpectedStateException e) {
      log.error(e);
    }
    return Collections.emptySet();
  }
  
  @Override
  public void register(AccumuloSchema<?> parentSchema) {
    this.schema = parentSchema;
    this.typeFactory = parentSchema.getTypeFactory();
    
  }
  
}
