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
import net.hydromatic.optiq.jdbc.DriverVersion;
import net.hydromatic.optiq.jdbc.OptiqConnection;
import net.hydromatic.optiq.jdbc.UnregisteredDriver;
import cosmos.sql.impl.CosmosSql;
import cosmos.sql.impl.CosmosTable;

/**
 * JDBC Driver.
 */
public class CosmosDriver extends UnregisteredDriver {
  private static final String CONNECTOR_STRING_PREFIX = "jdbc:accumulo:";
  
  public static final String COSMOS = "cosmos";
  
  protected SchemaDefiner<?> definer;
  private TableSchema<CosmosSql> schema;
  
  protected String jdbcConnector = null;
  
  protected CosmosDriver(String connectorName) {
    super();
    jdbcConnector = connectorName;
  }
  
  public CosmosDriver(SchemaDefiner<?> definer, String connectorPrefix) {
    this(connectorPrefix);
    this.definer = definer;
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
    OptiqConnection optiqConnection = (OptiqConnection) connection;
    
    final MutableSchema rootSchema = optiqConnection.getRootSchema();
    
    try {
      schema = new TableSchema<CosmosSql>(rootSchema, "cosmos", rootSchema.getSubSchemaExpression(COSMOS, TableSchema.class),
          (CosmosSql) definer, CosmosTable.class);
      
      schema.initialize();
      rootSchema.addSchema(COSMOS, schema);
      
      //TODO Pass through the user management operations through Cosmos to Accumulo
      //     given that I don't want to reimplement crap like kerberos/ldap
      // Map<String,Object> users = Maps.newHashMap();
      // users.put("admin", "changeme");
      //
      // BasicDataSource dataSource = new BasicDataSource();
      // dataSource.setUrl(getConnectStringPrefix() + "//" + url);
      // dataSource.setUsername("admin");
      // dataSource.setPassword("changeme");
      
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    return optiqConnection;
  }
  
}
