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

import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.eigenbase.reltype.RelDataType;

import cosmos.sql.TableSchema;
import cosmos.sql.DataTable;
import cosmos.sql.SchemaDefiner;

/**
 * Cosmos table representation.
 * 
 * 
 */
public class MetadataTable extends DataTable<Object[]> {
  
 
  
  public MetadataTable(TableSchema<? extends SchemaDefiner<?>> meataSchema, SchemaDefiner<?> metadata, JavaTypeFactory typeFactory, RelDataType rowType,
      String table) {
    
    super(meataSchema, table, typeFactory);
    
    super.table = table;
    
    super.rowType = rowType;
    
  }
  
  
  
}
