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
import java.util.Map;

import java.util.List;


import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.Schema.TableInSchema;
import net.hydromatic.optiq.impl.TableInSchemaImpl;
import net.hydromatic.optiq.impl.java.MapSchema;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeFactory.FieldInfoBuilder;

import com.google.common.collect.Lists;

import cosmos.options.Index;
import cosmos.results.CloseableIterable;
import cosmos.sql.impl.CosmosTable;
import cosmos.store.PersistedStores;
import cosmos.store.Store;

public class TableSchema<T extends SchemaDefiner<?>> extends MapSchema {
  
  protected T metaData;
  protected Connector connector;
  protected Authorizations auths;
  protected String metadataTable;
  
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
  public TableSchema(Schema parentSchema, String name, Expression expression, T schemaDefiner, Class<? extends DataTable<?>> clazz,
      Connector c, Authorizations auths, String metadataTable) {
    super(parentSchema, name, expression);
    metaData = schemaDefiner;
    // let's make a cyclic dependency
    metaData.register(this);
    
    
    if (metaData instanceof TableDefiner) {
        Collection<String> tables = ((TableDefiner) metaData).getTables();
        for(String table : tables)
        {
        	tableMap.put(table, new TableInSchema(parentSchema, table, null) {
				
				@Override
				public <E> Table<E> getTable(Class<E> elementType) {
					// TODO Auto-generated method stub
					return null;
				}
			});
        }
    }
    this.connector = c;
    this.auths = auths;
    this.metadataTable = metadataTable;
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
    try {
      CloseableIterable<Store> stores = PersistedStores.list(connector, auths, metadataTable);
      
      List<TableInSchema> tables = Lists.newArrayList();
      for (Store store : stores) {
        FieldInfoBuilder builder = new RelDataTypeFactory.FieldInfoBuilder();
  
        for (Index indexField : store.columnsToIndex()) {
          builder.add(indexField.column().name(), typeFactory.createType(indexField.getIndexTyped()));
        } 
  
        final RelDataType rowType = typeFactory.createStructType(builder);
  
        CosmosTable table = new CosmosTable(this, metaData, typeFactory, rowType, store.uuid());
        
        TableInSchema tas = new TableInSchemaImpl(this, store.uuid(), TableType.TABLE, table);
        
        tables.add(tas);
      }
      
      stores.close();

      return tables;
      
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
  
}
