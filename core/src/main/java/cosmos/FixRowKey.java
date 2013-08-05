package cosmos;

import java.util.Collections;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import cosmos.impl.CosmosImpl;


/**
 * 
 */
public class FixRowKey {

  public static void main(String[] args) throws Exception {
//    System.out.println(String.format("%011d", Integer.MAX_VALUE));
    ZooKeeperInstance inst = new ZooKeeperInstance("accumulo1.5", "localhost");
    Connector c = inst.getConnector("root", "secret");
    
    BatchScanner bs = c.createBatchScanner("sortswiki", c.securityOperations().getUserAuthorizations(c.whoami()), 2);
    BatchWriter bw = c.createBatchWriter("sortswiki2", 100*1024*1024, CosmosImpl.DEFAULT_MAX_LATENCY, 6);
    
    bs.setRanges(Collections.singleton(new Range()));
    
    final Text holder = new Text();
    final byte[] empty = new byte[0];
    for (Entry<Key,Value> e : bs) {
      e.getKey().getRow(holder);
      Integer i = Integer.parseInt(holder.toString());
      Mutation m = new Mutation(String.format("%011d", i));
//      m.put(empty, empty, e.getValue().get());
      bw.addMutation(m);
    }
    
    bs.close();
    bw.close();
  }
}
