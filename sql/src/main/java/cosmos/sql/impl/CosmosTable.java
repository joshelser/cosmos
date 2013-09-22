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
