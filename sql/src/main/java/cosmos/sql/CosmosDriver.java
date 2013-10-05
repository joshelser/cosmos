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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.Schema.TableInSchema;
import net.hydromatic.optiq.Schema.TableType;
import net.hydromatic.optiq.impl.TableInSchemaImpl;
import net.hydromatic.optiq.jdbc.DriverVersion;

import net.hydromatic.optiq.jdbc.CosmosConnection;
import net.hydromatic.optiq.jdbc.OptiqConnection;
import net.hydromatic.optiq.jdbc.OptiqStatement;
import net.hydromatic.optiq.jdbc.UnregisteredDriver;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

import cosmos.sql.impl.CosmosSql;
import cosmos.sql.impl.CosmosTable;
import cosmos.sql.impl.MetadataTable;

/**
 * JDBC Driver.
 */
public class CosmosDriver extends UnregisteredDriver {
  private static final String CONNECTOR_STRING_PREFIX = "jdbc:accumulo:";
  
  public static final String COSMOS = "cosmos";
  
  protected SchemaDefiner<?> definer;
  private TableSchema<CosmosSql> schema;
  
  protected String jdbcConnector = null;
  protected Connector connector = null;
  protected Authorizations auths = null;
  protected String metadataTable = null;
  
  protected CosmosDriver(String connectorName) {
    super();
    jdbcConnector = connectorName;
  }
  
  public CosmosDriver(SchemaDefiner<?> definer, String connectorPrefix, Connector connector, Authorizations auths, String metadataTable) {
    this(connectorPrefix);
    this.definer = definer;
    
    this.connector = connector;
    this.auths = auths;
    this.metadataTable = metadataTable;
    
    register();
  }
  
  /**
   * Returns the JDBC connector string using the given {@link connectorName}
   * 
   * @param connectorName
   * @return
   */
  public static String jdbcConnectionString(String connectorName) {
    return CONNECTOR_STRING_PREFIX + connectorName;
  }
  
  public static String jdbcConnectionString(CosmosDriver driver) {
    return jdbcConnectionString(driver.jdbcConnector);
  }
  
  protected String getConnectStringPrefix() {
    return CONNECTOR_STRING_PREFIX + jdbcConnector;
  }
  
  protected DriverVersion createDriverVersion() {
    return new CosmosJdbcDriverVersion(jdbcConnector);
  }
  
  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    Connection connection = super.connect(url, info);
    CosmosConnection optiqConnection = new CosmosConnection( (OptiqConnection) connection );
    
    final MutableSchema rootSchema = optiqConnection.getRootSchema();
    
    try {
    	
      schema = new TableSchema<CosmosSql>(rootSchema, COSMOS, rootSchema.getSubSchemaExpression(COSMOS, TableSchema.class),
          (CosmosSql) definer, CosmosTable.class, connector, auths, metadataTable);
            
      schema.initialize();
      
      
      
      rootSchema.addSchema(COSMOS, schema);
      
     
      
    } catch (Exception e) {
    	e.printStackTrace();
      throw new RuntimeException(e);
    }
    
    return optiqConnection;
  }
  
}
