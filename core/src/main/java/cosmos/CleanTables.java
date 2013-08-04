package cosmos;

import java.util.Collections;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Range;

import cosmos.options.Defaults;

/**
 * Clears out the tables without worrying about the permissions
 */
public class CleanTables {
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    ZooKeeperInstance zk = new ZooKeeperInstance("accumulo1.5", "localhost");
    Connector con = zk.getConnector("mediawiki", new PasswordToken("password"));

    BatchDeleter bd = con.createBatchDeleter(Defaults.DATA_TABLE, con.securityOperations().getUserAuthorizations("mediawiki"), 10, new BatchWriterConfig());
    
    bd.setRanges(Collections.singleton(new Range()));
    bd.delete();
    
    bd.close();
    
    con.tableOperations().compact(Defaults.DATA_TABLE, null, null, true, true);
    
    bd = con.createBatchDeleter(Defaults.METADATA_TABLE, con.securityOperations().getUserAuthorizations("mediawiki"), 10, new BatchWriterConfig());
    
    bd.setRanges(Collections.singleton(new Range()));
    bd.delete();
    
    bd.close();
    
    con.tableOperations().compact(Defaults.METADATA_TABLE, null, null, true, true);
  }
  
}
