package cosmos.sql.repl;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeSet;

import jline.console.ConsoleReader;
import jline.console.history.FileHistory;

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
  private static final String PROMPT = "cosmos> ";
  
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
    ConsoleReader reader = null;
    FileHistory history = null;
    
    try {
      reader = new ConsoleReader();
      PrintWriter sysout = new PrintWriter(reader.getOutput());
      
      String home = System.getProperty("HOME");
      if (null == home) {
        home = System.getenv("HOME");
      }
      
      // Get the home directory
      File homeDir = new File(home);
      
      // Make sure it actually exists
      if (homeDir.exists() && homeDir.isDirectory()) {
        // Check for, and create if necessary, the directory for cosmos to use
        File historyDir = new File(homeDir, ".cosmos");
        if (!historyDir.exists() && !historyDir.mkdirs()) {
          log.warn("Could not create directory for history at {}", historyDir);
        }
        
        // Get a file for jline history
        File historyFile = new File(historyDir, "history");
        history = new FileHistory(historyFile);
        reader.setHistory(history);
      } else {
        log.warn("Home directory not found: {}", homeDir);
      }
      
      String line;
      while ((line = reader.readLine(PROMPT)) != null) {
        line = StringUtils.strip(line);
        
        if (StringUtils.isBlank(line)) {
          continue;
        }
        
        if (EXIT.equals(line)) {
          break;
        }
        
        Statement statement = null;
        
        try {
          statement = connection.createStatement();
          
          final ResultSet resultSet = statement.executeQuery(line);
          final ResultSetMetaData metadata = resultSet.getMetaData();
          final int columnCount = metadata.getColumnCount();
          
          TreeSet<String> columns = Sets.newTreeSet();
          StringBuilder header = new StringBuilder(256);
          final String separator = "\t";
          final String newline = "\n";
          
          for (int i = 1; i <= columnCount; i++) {
            String columnName = metadata.getColumnName(i);
            columns.add(columnName);
            header.append(columnName);
            if (i < columnCount) {
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
          sysout.println(header);
          sysout.print(body);
          
        } catch (SQLException e) {
          log.error("SQLException", e);
          sysout.println();
        } catch (RuntimeException e) {
          log.error("RuntimeException", e);
          sysout.println();
        } finally {
          sysout.flush();
        }
      }
    } catch (IOException e) {
      log.error("Caught IOException", e);
    } finally {
      if (null != reader) {
        reader.shutdown();
      }
      
      if (null != history) {
        try {
          history.flush();
        } catch (IOException e) {
          log.warn("Couldn't flush history to disk", e);
        }
      }
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
      
      log.info("Started Accumulo MiniCluster");
      
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
