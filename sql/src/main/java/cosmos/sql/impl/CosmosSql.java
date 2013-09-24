/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 *  Copyright 2013 
 *
 */
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
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeFactory.FieldInfoBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import cosmos.Cosmos;
import cosmos.UnexpectedStateException;
import cosmos.UnindexedColumnException;
import cosmos.impl.Store;
import cosmos.options.Index;
import cosmos.results.Column;
import cosmos.results.SValue;
import cosmos.results.impl.MultimapQueryResult;
import cosmos.sql.AccumuloIterables;
import cosmos.sql.AccumuloRel;
import cosmos.sql.AccumuloTable;
import cosmos.sql.CosmosSchema;
import cosmos.sql.SchemaDefiner;
import cosmos.sql.TableDefiner;
import cosmos.sql.call.BaseVisitor;
import cosmos.sql.call.CallIfc;
import cosmos.sql.call.ChildVisitor;
import cosmos.sql.call.Field;
import cosmos.sql.call.Fields;
import cosmos.sql.call.impl.Filter;
import cosmos.sql.impl.functions.FieldLimiter;

/**
 * Cosmos SQL defines a table and a schema, therefore it is capable of returning an iterator of results for a given expression
 */
public class CosmosSql implements SchemaDefiner<Object[]>, TableDefiner {

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

  private CosmosSchema<?> schema;

  private static final Logger log = LoggerFactory.getLogger(CosmosSql.class);

  protected Cache<String,CosmosTable> tableCache = CacheBuilder.newBuilder().expireAfterAccess(24, TimeUnit.HOURS).build();

  /**
   * Constructor
   */

  public CosmosSql(Cosmos cosmosImpl) throws MutationsRejectedException, TableNotFoundException, UnexpectedStateException {
    Preconditions.checkNotNull(cosmosImpl);
    
    plannedParentHood = Lists.newArrayList();
    iter = Collections.emptyList();
    this.cosmos = cosmosImpl;

  }

  @Override
  public String getDataTable() {
    return "";

  }

  @SuppressWarnings("unchecked")
  @Override
  public AccumuloIterables<Object[]> iterator(List<String> schemaLayout, AccumuloRel.Plan planner, AccumuloRel.Plan aggregatePlan) {

    plannedParentHood = Lists.newArrayList();
    iter = Collections.emptyList();
    Iterator<Object[]> returnIter = Iterators.emptyIterator();

    BaseVisitor<? extends CallIfc<?>> query = planner.getChildren();

    Collection<? extends CallIfc<?>> filters = query.children(Filter.class.getSimpleName());

    Collection<? extends CallIfc<?>> fields = query.children("selectedFields");

    String table = ((CosmosTable) planner.table).getTable();

    Store res;
    try {

      res = cosmos.fetch(table);
      if (aggregatePlan == null) {

        if (res != null) {
          if (filters.size() > 0) {
            for (CallIfc<?> filterIfc : filters) {
              Filter filter = (Filter) filterIfc;

                plannedParentHood.add(buildFilterIterator(filter.getFilters(), res));

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
      log.error("Could not group results", e1);
    } catch (TableNotFoundException e) {
      log.error("Could not group results", e);
    } catch (UnindexedColumnException e) {
      log.error("Could not group results", e);
    }

    return new AccumuloIterables<Object[]>(returnIter);
  }

  private Iterable<MultimapQueryResult> buildFilterIterator(List<ChildVisitor> filters, Store res) {

    Iterable<MultimapQueryResult> baseIter = Collections.emptyList();
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
        Store sort = cosmos.fetch(name);

        FieldInfoBuilder builder = new RelDataTypeFactory.FieldInfoBuilder();

        for (Index indexField : sort.columnsToIndex()) {
          builder.add(indexField.column().name(), typeFactory.createType(indexField.getIndexTyped()));
        }

        final RelDataType rowType = typeFactory.createStructType(builder);

        try {
          table = new CosmosTable(schema, this, typeFactory, rowType, name);

          tableCache.put(name, table);
        } catch (SecurityException e) {
          log.error("Could not create CosmosTable", e);
        }
      }

    } catch (UnexpectedStateException e) {
      log.error("Couldn't find result in Cosmos", e);

    }

    return table;
  }

  @Override
  public Set<Index> getIndexColumns(String table) {
    Store sort;
    try {
      sort = cosmos.fetch(table);

      if (sort != null) {
        return sort.columnsToIndex();
      }

    } catch (UnexpectedStateException e) {
      log.error("Could not find result in Cosmos", e);
    }
    return Collections.emptySet();
  }

  @Override
  public void register(CosmosSchema<?> parentSchema) {
    this.schema = parentSchema;
    this.typeFactory = parentSchema.getTypeFactory();

  }

}
