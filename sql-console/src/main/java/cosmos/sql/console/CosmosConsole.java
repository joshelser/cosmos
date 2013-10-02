package cosmos.sql.console;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import jline.console.ConsoleReader;
import jline.console.history.FileHistory;
import jline.console.history.History;
import jline.console.history.MemoryHistory;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.mediawiki.xml.export_0.MediaWikiType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import cosmos.Cosmos;
import cosmos.impl.CosmosImpl;
import cosmos.options.Defaults;
import cosmos.results.QueryResult;
import cosmos.results.integration.CosmosIntegrationSetup;
import cosmos.sql.CosmosDriver;
import cosmos.sql.impl.CosmosSql;
import cosmos.store.PersistedStores;
import cosmos.store.Store;

public class CosmosConsole {
  private static final Logger log = LoggerFactory.getLogger(CosmosConsole.class);
  private static final String EXIT = "exit";
  private static final String PROMPT = "cosmos> ";
  
  public static class CosmosConsoleOptions {
    @Parameter(names = {"--use-mini", "-m"}, description = "Start an Accumulo MiniCluster")
    public boolean useMiniCluster = false;
    
    @Parameter(names = {"--use-real", "-r"}, description = "Connecto to a live Accumulo instance")
    public boolean useRealInstance = false;
    
    @Parameter(names = {"--loaddata", "-l"}, description = "Load some data into the Accumulo instance")
    public boolean loadData = false;
    
    @Parameter(names = {"--help", "-h"}, description = "Prints a help message", help = true)
    public boolean help = false;
    
    public CosmosConsoleOptions() {}
  }
  
  public static class AccumuloInstanceOptions {
    @Parameter(names = {"--zookeepers", "-z"}, description = "CSV of zookeepers to use")
    public String zookeepers;
    
    @Parameter(names = {"--instance", "-i"}, description = "Accumulo instance name")
    public String instanceName;
    
    @Parameter(names = {"--username", "-u"}, description = "Accumulo user name")
    public String username;
    
    @Parameter(names = {"--password", "-p"}, description = "Accumulo user password")
    public String password;
    
    @Parameter(names = {"--id", "-s"}, description = "The UUID of the SortableResult to use")
    public String uuid;
  }
  
  protected Cosmos cosmos;
  protected Connection connection;
  
  public CosmosConsole(Cosmos c, Connection connection) {
    this.cosmos = c;
    this.connection = connection;
  }
  
  /**
   * Load the History from disk if a $HOME directory is defined, otherwise fall back to using an in-memory history object
   * 
   * @return
   * @throws IOException
   */
  protected History getJLineHistory() throws IOException {
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
        log.warn("Could not create directory for history at {}, using temporary history.", historyDir);
      }
      
      // Get a file for jline history
      File historyFile = new File(historyDir, "history");
      return new FileHistory(historyFile);
    } else {
      log.warn("Home directory not found: {}, using temporary history", homeDir);
      return new MemoryHistory();
    }
  }
  
  public void startRepl() {
    ConsoleReader reader = null;
    History history = null;
    final TableOutputFormat tableOutput = new TableOutputFormat();
    
    try {
      reader = new ConsoleReader();
      PrintWriter sysout = new PrintWriter(reader.getOutput());
      
      // Try to load the history and set it on the ConsoleReader
      history = getJLineHistory();
      if (null != history) {
        reader.setHistory(history);
      }
      
      String line;
      while ((line = reader.readLine(PROMPT)) != null) {
        line = StringUtils.strip(line);
        
        // Eat a blank line
        if (StringUtils.isBlank(line)) {
          continue;
        }
        
        // Quit on `exit`
        if (EXIT.equals(line)) {
          break;
        }
        
        Statement statement = null;
        
        try {
          statement = connection.createStatement();
          
          Stopwatch timer = new Stopwatch();
          timer.start();
          final ResultSet resultSet = statement.executeQuery(line);
          tableOutput.print(resultSet, timer);
          
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
        if (FileHistory.class.isAssignableFrom(history.getClass())) {
          try {
            ((FileHistory) history).flush();
          } catch (IOException e) {
            log.warn("Couldn't flush history to disk", e);
          }
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
    
    log.info("Starting Cosmos Console");
    
    // Make sure optiq can load the class we need for sql
    try {
      Class.forName(CosmosDriver.class.getCanonicalName());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("driver not found", e);
    }
    
    try {
      CosmosConsoleOptions consoleOptions = new CosmosConsoleOptions();
      AccumuloInstanceOptions accumuloOptions = new AccumuloInstanceOptions();
      new JCommander(new Object[] {consoleOptions, accumuloOptions}, args);
      
      Store id = null;
      Connector connector = null;
      
      Authorizations auths = new Authorizations("en");
      
      if (consoleOptions.useMiniCluster) {
        log.info("Starting Accumulo MiniCluster");
        
        MiniAccumuloConfig macConf = new MiniAccumuloConfig(tmp, passwd);
        macConf.setNumTservers(2);
        
        mac = new MiniAccumuloCluster(macConf);
        
        mac.start();
        
        cosmos = new CosmosImpl(mac.getZooKeepers());
        
        ZooKeeperInstance instance = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
        connector = instance.getConnector("root", passwd);
        
        // Set this since we know we need it for the wiki test data
        connector.securityOperations().changeUserAuthorizations("root", auths);
      } else if (consoleOptions.useRealInstance) {
        if (null == accumuloOptions.instanceName || null == accumuloOptions.zookeepers || null == accumuloOptions.username || null == accumuloOptions.password) {
          log.error("If an ID for preloaded data is provided, connection information must also be provided");
          System.exit(1);
          return;
        }
        
        ZooKeeperInstance instance = new ZooKeeperInstance(accumuloOptions.instanceName, accumuloOptions.zookeepers);
        connector = instance.getConnector(accumuloOptions.username, accumuloOptions.password);
        
        cosmos = new CosmosImpl(accumuloOptions.zookeepers);
      } else {
        log.error("You must choose to use either an Accumulo minicluster or a real Accumulo instance");
        System.exit(1);
      }
      
      if (consoleOptions.loadData) {
        log.info("Loading wiki data");
        
        // Pre-load jaxb
        CosmosIntegrationSetup.initializeJaxb();
        
        // Load all of the wikis.. they're not *that* big
        List<QueryResult<?>> inputData = Lists.newArrayListWithExpectedSize(200000);
        for (MediaWikiType wiki : Lists.newArrayList(CosmosIntegrationSetup.getWiki1(), CosmosIntegrationSetup.getWiki2(), CosmosIntegrationSetup.getWiki3(),
            CosmosIntegrationSetup.getWiki4(), CosmosIntegrationSetup.getWiki5())) {
          inputData.addAll(CosmosIntegrationSetup.wikiToMultimap(wiki));
        }
        
        id = new Store(connector, connector.securityOperations().getUserAuthorizations("root"), CosmosIntegrationSetup.ALL_INDEXES);
        
        // Override the default BatchWriterConfig (50M memory buffer)
        // BatchWriterConfig bwConfig = new BatchWriterConfig();
        // bwConfig.setMaxMemory(1024 * 1024 * 100);
        // id.setWriterConfig(bwConfig);
        
        log.info("Sending data through Cosmos");
        
        cosmos.register(id);
        cosmos.addResults(id, inputData);
        cosmos.finalize(id);
        
        log.info("Loaded wiki data with an id of {}", id.uuid());
        
        // Serialize this Store so we can reconstitute it again later
        PersistedStores.store(id);
      }
      
      cosmosSql = new CosmosSql(cosmos, connector, Defaults.METADATA_TABLE, auths);
      
      CosmosDriver driver = new CosmosDriver(cosmosSql, "cosmos", connector, auths, Defaults.METADATA_TABLE);
      
      Connection connection = DriverManager.getConnection(CosmosDriver.jdbcConnectionString(driver) + "//localhost", new Properties());
      
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
      
      FileUtils.deleteDirectory(tmp);
    }
  }
}
