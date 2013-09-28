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

import java.util.Collection;

import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.java.MapSchema;

import com.google.common.collect.Lists;

public class TableSchema<T extends SchemaDefiner<?>> extends MapSchema {

  protected T metaData;

  /**
   * Constructor to define a cosmos schema
   * 
   * @param parentSchema
   *          parent schema
   * @param name
   *          name of the schema
   * @param expression
   *          expression used for the callback
   * @param schemaDefiner
   * @param clazz
   */
  public TableSchema(Schema parentSchema, String name, Expression expression, T schemaDefiner, Class<? extends DataTable<?>> clazz) {
    super(parentSchema, name, expression);
    metaData = schemaDefiner;
    // let's make a cyclic dependency
    metaData.register(this);
  }

  /**
   * Returns the table associated with the class
   */
  public DataTable<?> getTable(String name) {
    if (metaData instanceof TableDefiner) {
      return ((TableDefiner) metaData).getTable(name);
    } else
      return (DataTable<?>) tableMap.get(name).getTable(Class.class);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <E> Table<E> getTable(String name, Class<E> elementType) {
    return (Table<E>) getTable(name);
  }

  @Override
  protected Collection<TableInSchema> initialTables() {
    return Lists.newArrayList();

  }
}
