package cosmos.sql.console;

import java.util.List;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.mediawiki.xml.export_0.MediaWikiType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Sets;

import cosmos.Cosmos;
import cosmos.impl.CosmosImpl;
import cosmos.options.Index;
import cosmos.results.QueryResult;
import cosmos.results.integration.CosmosIntegrationSetup;
import cosmos.store.Store;

public class LoadWikiData {
  private static final Logger log = LoggerFactory.getLogger(LoadWikiData.class);
  
  @Parameter(names = {"--zookeepers", "-z"}, description = "CSV of zookeepers to use", required = true)
  public String zookeepers;
  
  @Parameter(names = {"--instance", "-i"}, description = "Accumulo instance name", required = true)
  public String instanceName;
  
  @Parameter(names = {"--username", "-u"}, description = "Accumulo user name", required = true)
  public String username;
  
  @Parameter(names = {"--password", "-p"}, description = "Accumulo user password", required = true)
  public String password;
  
  public static void main(String[] args) throws Exception {
    LoadWikiData obj = new LoadWikiData();
    
    new JCommander(obj, args);
    
    log.info("Connecting to ZooKeeper");
    ZooKeeperInstance instance = new ZooKeeperInstance(obj.instanceName, obj.zookeepers);
    
    log.info("Connecting to Accumulo");
    Connector connector = instance.getConnector(obj.username, new PasswordToken(obj.password));
    
    log.info("Instantiating Cosmos");
    Cosmos cosmos = new CosmosImpl(obj.zookeepers);
    
    Store id = new Store(connector, connector.securityOperations().getUserAuthorizations(obj.username), CosmosIntegrationSetup.ALL_INDEXES);
    
    cosmos.register(id);
    
    log.info("Parsing mediawiki data");
    MediaWikiType wiki = CosmosIntegrationSetup.getWiki1();
    
    List<QueryResult<?>> results = CosmosIntegrationSetup.wikiToMultimap(wiki);
    
    log.info("Loading {} mediawiki pages as {}", results.size(), id.uuid());
    cosmos.addResults(id, results);
    
    cosmos.finalize(id);
    
    log.info("Finished loading");
    cosmos.close();
  }
}
