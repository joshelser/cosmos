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
package cosmos.sql;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import net.hydromatic.linq4j.AbstractQueryable;
import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.Linq4j;
import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.Statistic;
import net.hydromatic.optiq.Statistics;
import net.hydromatic.optiq.TranslatableTable;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import com.google.common.collect.Queues;

import cosmos.sql.AccumuloRel.Plan;

/**
 * Accumulo table representation. Is a queryable mechanism
 * 
 * @param <T>
 */
public abstract class AccumuloTable<T> extends AbstractQueryable<T> implements TranslatableTable<T> {

  protected CosmosSchema<? extends SchemaDefiner<?>> schema;

  protected String tableName;

  protected AccumuloIterables<T> resultSet;

  /**
   * plans that perform aggreation
   */
  protected Queue<Plan> aggregationPlans;

  /**
   * Queue of plans to execute
   */
  protected Queue<Plan> plans;

  public AccumuloTable(final CosmosSchema<? extends SchemaDefiner<?>> schema, final String tableName, JavaTypeFactory typeFactory) {
    this.schema = schema;
    this.tableName = tableName;
    plans = Queues.newPriorityQueue();
    aggregationPlans = Queues.newPriorityQueue();
    resultSet = new AccumuloIterables<T>();

  }

  @Override
  public Expression getExpression() {
    return Expressions.convert_(Expressions.call(schema.getExpression(), "getTable", Expressions.constant(tableName), Expressions.constant(getElementType())),
        AccumuloTable.class);

  }

  @Override
  public QueryProvider getProvider() {
    return schema.getQueryProvider();
  }

  @Override
  public Iterator<T> iterator() {
    return Linq4j.enumeratorIterator(enumerator());
  }

  @Override
  public DataContext getDataContext() {
    return schema;
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  public boolean groupBy(Plan plan) {
    return aggregationPlans.add(plan);
  }

  public boolean enqueue(Plan plan) {
    return plans.add(plan);
  }

  public abstract Enumerable<T> accumulate(List<String> fieldNameList);

  @Override
  public Enumerator<T> enumerator() {

    return Linq4j.enumerator(new ArrayList<T>());
  }

  public void execute(Plan relationalExpression) {

  }

}
