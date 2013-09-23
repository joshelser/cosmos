package cosmos.sql.repl;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import jline.console.ConsoleReader;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.lang.StringUtils;
import org.mediawiki.xml.export_0.MediaWikiType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import cosmos.Cosmos;
import cosmos.impl.CosmosImpl;
import cosmos.impl.SortableResult;
import cosmos.options.Index;
import cosmos.results.Column;
import cosmos.results.QueryResult;
import cosmos.results.SValue;
import cosmos.results.integration.CosmosIntegrationSetup;
import cosmos.sql.CosmosDriver;
import cosmos.sql.impl.CosmosSql;

public class CosmosConsole {
  private static final Logger log = LoggerFactory.getLogger(CosmosConsole.class);
  private static final String EXIT = "exit";
  private static final String PROMPT = "cosmos > ";
  
  public static final String JDBC_URL = "https://localhost:8089";
  public static final String USER = "admin";
  public static final String PASSWORD = "changeme";
  
  protected Cosmos cosmos;
  protected Connection connection;
  
  public CosmosConsole(Cosmos c, Connection connection) {
    this.cosmos = c;
    this.connection = connection;
  }
  
  public void startRepl() {
    try {
      ConsoleReader reader = new ConsoleReader();
      
      String line;
      while ((line = reader.readLine(PROMPT)) != null) {
        line = StringUtils.strip(line);
        if (EXIT.equals(line)) {
          break;
        }
        
        Statement statement = null;
        
        try {
          statement = connection.createStatement();
          
          final ResultSet resultSet = statement.executeQuery(line);
          final ResultSetMetaData metadata = resultSet.getMetaData();
          final int columnCount = metadata.getColumnCount();
          
          List<String> columns = Lists.newArrayListWithExpectedSize(columnCount);
          StringBuilder header = new StringBuilder(256);
          final String separator = "\t";
          final String newline = "\n";
          
          for (int i = 1; i <= columnCount; i++) {
            String columnName = metadata.getColumnName(i);
            columns.add(columnName);
            header.append(columnName);
            if (i <= columnCount - 1) {
              header.append(separator);
            }
          }
          
          final StringBuilder body = new StringBuilder(256);
          while (resultSet.next()) {
            for (String column : columns) {
              List<Entry<Column,SValue>> sValues = (List<Entry<Column,SValue>>) resultSet.getObject(column);
              
              if (null != sValues && !sValues.isEmpty()) {
                for (Entry<Column,SValue> values : sValues) {
                  body.append(values.getValue().toString());
                }
              } else {
                body.append(" ");
              }
              
              body.append(separator);
            }
            
            // If we inserted anything, remove the last separator
            if (body.length() > 0) {
              body.setLength(body.length() - separator.length());
            }
            
            body.append(newline);
          }
          
          // Print out the header and then the body (we already have an extra newline on the body)
          System.out.println(header);
          System.out.print(body);
          
        } catch (SQLException e) {
          log.error("SQLException", e);
          System.out.println();
        } catch (RuntimeException e) {
          log.error("RuntimeException", e);
          System.out.println();
        }
      }
    } catch (IOException e) {
      log.error("Caught IOException", e);
    } finally {
      
    }
  }
  
  public static void main(String[] args) throws Exception {
    final File tmp = Files.createTempDir();
    final String passwd = "foobar";
    
    MiniAccumuloCluster mac = null;
    Cosmos cosmos = null;
    CosmosSql cosmosSql = null;
    
    // Make sure optiq can load the class we need for sql
    try {
      Class.forName(CosmosDriver.class.getCanonicalName());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("driver not found", e);
    }
    
    try {
      
      MiniAccumuloConfig macConf = new MiniAccumuloConfig(tmp, passwd);
      macConf.setNumTservers(1);
      
      mac = new MiniAccumuloCluster(macConf);
      
      mac.start();
      
      // Pre-load jaxb
      CosmosIntegrationSetup.initializeJaxb();
      
      MediaWikiType wiki1 = CosmosIntegrationSetup.getWiki1();
      List<QueryResult<?>> results1 = CosmosIntegrationSetup.wikiToMultimap(wiki1);
      
      cosmos = new CosmosImpl(mac.getZooKeepers());
      
      ZooKeeperInstance instance = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
      Connector connector = instance.getConnector("root", new PasswordToken(passwd));
      
      connector.securityOperations().changeUserAuthorizations("root", new Authorizations("en"));
      
      SortableResult id = new SortableResult(connector, connector.securityOperations().getUserAuthorizations("root"), 
          Sets.newHashSet(Index.define("PAGE_ID"), Index.define("PAGE_TITLE"), Index.define("CONTRIBUTOR_IP"), Index.define("CONTRIBUTOR_USERNAME"),
              Index.define("CONTRIBUTOR_ID"), Index.define("REVISION_ID"), Index.define("REVISION_TIMESTAMP"), Index.define("REVISION_COMMENT")));
      
      cosmos.register(id);
      cosmos.addResults(id, results1);
      cosmos.finalize(id);
      
      log.info("Loaded wiki data with an id of {}", id.uuid());
      
      cosmosSql = new CosmosSql(cosmos);
      
      new CosmosDriver(cosmosSql, "cosmos");
      
      Properties info = new Properties();
      info.put("url", JDBC_URL);
      info.put("user", USER);
      info.put("password", PASSWORD);
      Connection connection = DriverManager.getConnection("jdbc:accumulo:cosmos//localhost", info);
      
      CosmosConsole console = new CosmosConsole(cosmos, connection);
      
      console.startRepl();
    } finally {
      if (null != cosmos) {
        cosmos.close();
      }
      
      try {
        if (null != mac) {
          mac.stop();
        }
      } catch (Exception e) {
        log.error("Error stopping Accumulo minicluster", e);
      }
      
//      FileUtils.deleteDirectory(tmp);
    }
  }
}
