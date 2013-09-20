package cosmos.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.jdbc.DriverVersion;
import net.hydromatic.optiq.jdbc.OptiqConnection;
import net.hydromatic.optiq.jdbc.UnregisteredDriver;

import org.apache.commons.dbcp.BasicDataSource;

import com.google.common.collect.Maps;

import cosmos.sql.impl.CosmosSql;
import cosmos.sql.impl.CosmosTable;

/**
 * JDBC Driver.
 */
public class CosmosDriver extends UnregisteredDriver {
  
  public static final String COSMOS = "cosmos";
  protected SchemaDefiner<?> definer;
  private CosmosSchema<CosmosSql> schema;
  
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
  
  protected String getConnectStringPrefix() {
    return "jdbc:accumulo:" + jdbcConnector;
  }
  
  protected DriverVersion createDriverVersion() {
    return new CosmosJdbcDriverVersion(jdbcConnector);
  }
  
  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    Connection connection = super.connect(url, info);
    OptiqConnection optiqConnection = (OptiqConnection) connection;
    
    final MutableSchema rootSchema = optiqConnection.getRootSchema();
    
    // optiqConnection.setSchema("");
    try {
      schema = new CosmosSchema<CosmosSql>(rootSchema, "cosmos", rootSchema.getSubSchemaExpression(COSMOS, CosmosSchema.class),
          (CosmosSql) definer, CosmosTable.class);
      
      schema.initialize();
      rootSchema.addSchema(COSMOS, schema);
      
      Map<String,Object> users = Maps.newHashMap();
      users.put("admin", "changeme");
      
      BasicDataSource dataSource = new BasicDataSource();
      dataSource.setUrl(getConnectStringPrefix() + "//localhost");
      dataSource.setUsername("admin");
      dataSource.setPassword("changeme");
      
      // JdbcSchema.create(rootSchema,dataSource,"admin","",schemaName);
      
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    return optiqConnection;
  }
  
}

// End SplunkDriver.java
