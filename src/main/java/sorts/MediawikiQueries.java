package sorts;

import java.util.Collections;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;

import sorts.impl.SortableResult;
import sorts.impl.SortingImpl;
import sorts.mediawiki.MediawikiPage.Page;
import sorts.mediawiki.MediawikiPage.Page.Revision;
import sorts.mediawiki.MediawikiPage.Page.Revision.Contributor;
import sorts.options.Defaults;
import sorts.options.Index;
import sorts.results.CloseableIterable;
import sorts.results.Column;
import sorts.results.SValue;
import sorts.results.impl.MultimapQueryResult;
import sorts.util.IdentitySet;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * 
 */
public class MediawikiQueries {
  public static final int MAX_SIZE = 16000;
  public static final int MAX_OFFSET = 11845576 - MAX_SIZE;
  public static final int MAX_ROW = 999999999;
  
  public static final ColumnVisibility cv = new ColumnVisibility("en");
  
  public static final Column PAGE_ID = Column.create("PAGE_ID"), REVISION_ID = Column.create("REVISION_ID"), REVISION_TIMESTAMP = Column
      .create("REVISION_TIMESTAMP"), CONTRIBUTOR_USERNAME = Column.create("CONTRIBUTOR_USERNAME"), CONTRIBUTOR_ID = Column.create("CONTRIBUTOR_ID");
  
  public static MultimapQueryResult pagesToQueryResult(Page p) {
    HashMultimap<Column,SValue> data = HashMultimap.create();
    
    String pageId = Long.toString(p.getId());
    
    data.put(PAGE_ID, SValue.create(pageId, cv));
    
    Revision r = p.getRevision();
    if (null != r) {
      data.put(REVISION_ID, SValue.create(Long.toString(r.getId()), cv));
      data.put(REVISION_TIMESTAMP, SValue.create(r.getTimestamp(), cv));
      
      Contributor c = r.getContributor();
      if (null != c) {
        if (null != c.getUsername()) {
          data.put(CONTRIBUTOR_USERNAME, SValue.create(c.getUsername(), cv));
        }
        
        if (0l != c.getId()) {
          data.put(CONTRIBUTOR_ID, SValue.create(Long.toString(c.getId()), cv));
        }
      }
    }
    
    return new MultimapQueryResult(data, pageId, cv);
  }
  
  protected final Connector con;
  protected final Sorting sorts;
  
  public MediawikiQueries() throws Exception {
    ZooKeeperInstance zk = new ZooKeeperInstance("accumulo1.5", "localhost");
    this.con = zk.getConnector("mediawiki", new PasswordToken("password"));
    
    this.sorts = new SortingImpl("localhost");
  }
  
  public void run(int numIterations) throws Exception {
    final Random offsetR = new Random(), cardinalityR = new Random();
    
    int iters = 0;
    
    while (iters < numIterations) {
      SortableResult id = SortableResult.create(this.con, 
          this.con.securityOperations().getUserAuthorizations(this.con.whoami()), 
          IdentitySet.<Index> create());
      
      int offset = offsetR.nextInt(MAX_OFFSET);
      int numRecords = cardinalityR.nextInt(MAX_SIZE);
      
      BatchScanner bs = this.con.createBatchScanner("sortswiki", new Authorizations(), 4);
      
      bs.setRanges(Collections.singleton(new Range(Integer.toString(offset), Integer.toString(MAX_ROW))));
      
      Iterable<Entry<Key,Value>> inputIterable = Iterables.limit(bs, numRecords);
      
      this.sorts.register(id);
      
      System.out.println("Iteration " + iters);
      final AtomicInteger recordsReturned = new AtomicInteger(0);
      
      this.sorts.addResults(id, Iterables.transform(inputIterable, new Function<Entry<Key,Value>,MultimapQueryResult>() {
        @Override
        public MultimapQueryResult apply(Entry<Key,Value> input) {
          recordsReturned.incrementAndGet();
          Page p;
          try {
            p = Page.parseFrom(input.getValue().get());
          } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
          }
          return pagesToQueryResult(p);
        }
      }));
      
      System.out.println("Fetched " + recordsReturned + "/" + numRecords);
      bs.close();
      
      Stopwatch sw = new Stopwatch();
      int prev = Integer.MIN_VALUE;
      sw.start();
      
//      final CloseableIterable<MultimapQueryResult> results = this.sorts.fetch(id, Index.define(Defaults.DOCID_FIELD_NAME));
      final CloseableIterable<MultimapQueryResult> results = this.sorts.fetch(id, Index.define(REVISION_ID));
      for (MultimapQueryResult r : results) {
        int current = Integer.parseInt(r.docId());
        if (prev > current) {
          System.out.println("WOAH, got " + current + " docid which was greater than the previous " + prev);
          results.close();
          System.exit(1);
        }
        
        prev = current;
      }
      
      sw.stop();
      
      System.out.println("Took " + sw.toString() + " to fetch results");
      
      results.close();

      sw = new Stopwatch();
      sw.start();
      this.sorts.delete(id);
      sw.stop();
      System.out.println("Took " + sw.toString() + " to delete results\n");
      
      iters++;
    }
  }
  
  public static void main(String[] args) throws Exception {
    MediawikiQueries queries = new MediawikiQueries();
    
    queries.run(10);
  }
}
