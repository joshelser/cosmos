package cosmos.results;

import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import cosmos.Cosmos;
import cosmos.impl.CosmosImpl;
import cosmos.options.Defaults;
import cosmos.options.Index;
import cosmos.results.impl.MultimapQueryResult;
import cosmos.store.PersistedStores;
import cosmos.store.Store;
import cosmos.util.IdentitySet;

public class SerializedStoreTest extends AbstractSortableTest {
  
  @Test
  public void serializeStoreFetchOnly() throws Exception {
    Store id = Store.create(c, AUTHS, IdentitySet.<Index> create());
    String uuid = id.uuid();
    
    Cosmos cosmos = new CosmosImpl(zkConnectString());
    
    cosmos.register(id);
    
    // Save off our Store that we've defined
    PersistedStores.store(id);
    
    Multimap<Column,SValue> data = HashMultimap.create();
    
    data.put(Column.create("NAME"), SValue.create("George", VIZ));
    data.put(Column.create("AGE"), SValue.create("25", VIZ));
    data.put(Column.create("HEIGHT"), SValue.create("70", VIZ));
    
    MultimapQueryResult mqr = new MultimapQueryResult(data, "1", VIZ);
    
    cosmos.addResult(id, mqr);
    cosmos.finalize(id);
    
    id = null;
    
    id = PersistedStores.retrieve(c, Defaults.METADATA_TABLE, AUTHS, uuid);
    
    CloseableIterable<MultimapQueryResult> records = cosmos.fetch(id);
    Iterator<MultimapQueryResult> iter = records.iterator();
    Assert.assertTrue(iter.hasNext());
    
    MultimapQueryResult record = iter.next();
    
    Assert.assertEquals("1", record.docId());
    
    Assert.assertFalse(iter.hasNext());
    
    records.close();
    cosmos.close();
  }
  
  
  @Test
  public void serializeStoreFetchAndAdd() throws Exception {
    Store id = Store.create(c, AUTHS, IdentitySet.<Index> create());
    String uuid = id.uuid();
    
    Cosmos cosmos = new CosmosImpl(zkConnectString());
    
    cosmos.register(id);
    
    // Save off our Store that we've defined
    PersistedStores.store(id);
    
    Multimap<Column,SValue> data = HashMultimap.create();
    
    data.put(Column.create("NAME"), SValue.create("George", VIZ));
    data.put(Column.create("AGE"), SValue.create("25", VIZ));
    data.put(Column.create("HEIGHT"), SValue.create("70", VIZ));
    
    MultimapQueryResult mqr = new MultimapQueryResult(data, "1", VIZ);
    
    cosmos.addResult(id, mqr);
    
    id = null;
    
    id = PersistedStores.retrieve(c, Defaults.METADATA_TABLE, AUTHS, uuid);
    
    CloseableIterable<MultimapQueryResult> records = cosmos.fetch(id);
    Iterator<MultimapQueryResult> iter = records.iterator();
    Assert.assertTrue(iter.hasNext());
    
    MultimapQueryResult record = iter.next();
    
    Assert.assertEquals("1", record.docId());
    
    Assert.assertFalse(iter.hasNext());
    
    mqr = new MultimapQueryResult(data, "2", VIZ);
    cosmos.addResult(id, mqr);
    
    records = cosmos.fetch(id);
    iter = records.iterator();
    Assert.assertTrue(iter.hasNext());
    
    record = iter.next();
    
    Assert.assertEquals("1", record.docId());
    
    Assert.assertTrue(iter.hasNext());
    record = iter.next();
    
    Assert.assertEquals("2", record.docId());
    
    Assert.assertFalse(iter.hasNext());
    
    records.close();
    cosmos.close();
  }
  
}
