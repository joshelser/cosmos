package cosmos.store;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import cosmos.options.Defaults;
import cosmos.options.Index;
import cosmos.results.CloseableIterable;
import cosmos.util.AscendingIndexIdentitySet;
import cosmos.util.DescendingIndexIdentitySet;
import cosmos.util.IdentitySet;

public class PersistedStoresTest {
  
  private static class DummyConnector extends Connector {
    
    @Override
    public BatchScanner createBatchScanner(String tableName, Authorizations authorizations, int numQueryThreads) throws TableNotFoundException {
      return null;
    }
    
    @Override
    public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads, long maxMemory, long maxLatency,
        int maxWriteThreads) throws TableNotFoundException {
      return null;
    }
    
    @Override
    public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads, BatchWriterConfig config)
        throws TableNotFoundException {
      return null;
    }
    
    @Override
    public BatchWriter createBatchWriter(String tableName, long maxMemory, long maxLatency, int maxWriteThreads) throws TableNotFoundException {
      return null;
    }
    
    @Override
    public BatchWriter createBatchWriter(String tableName, BatchWriterConfig config) throws TableNotFoundException {
      return null;
    }
    
    @Override
    public MultiTableBatchWriter createMultiTableBatchWriter(long maxMemory, long maxLatency, int maxWriteThreads) {
      return null;
    }
    
    @Override
    public MultiTableBatchWriter createMultiTableBatchWriter(BatchWriterConfig config) {
      return null;
    }
    
    @Override
    public Scanner createScanner(String tableName, Authorizations authorizations) throws TableNotFoundException {
      return null;
    }
    
    @Override
    public Instance getInstance() {
      return null;
    }
    
    @Override
    public String whoami() {
      return null;
    }
    
    @Override
    public TableOperations tableOperations() {
      return null;
    }
    
    @Override
    public SecurityOperations securityOperations() {
      return null;
    }
    
    @Override
    public InstanceOperations instanceOperations() {
      return null;
    }
    
  }
  
  private final DummyConnector dummyConnector = new DummyConnector();
  protected Connector mockConnector;
  
  @Before
  public void setupMockAccumulo() throws Exception {
    MockInstance instance = new MockInstance();
    mockConnector = instance.getConnector("foo", new PasswordToken("password"));
    mockConnector.tableOperations().create(Defaults.METADATA_TABLE);
  }
  
  @Test
  public void storeEqualityTest() throws Exception {
    Authorizations auths = new Authorizations();
    IdentitySet<Index> index = IdentitySet.<Index> create();
    Store a = Store.create(dummyConnector, auths, index);
    
    Value v = PersistedStores.serialize(a);
    Store b = PersistedStores.deserialize(dummyConnector, v);
    
    Assert.assertEquals(a, b);
  }
  
  @Test
  public void storeInequalityTest() throws Exception {
    Authorizations auths = new Authorizations();
    IdentitySet<Index> index = IdentitySet.<Index> create();
    Store a = Store.create(dummyConnector, auths, "barfoo", index);
    Store b = Store.create(dummyConnector, auths, "foobar", index);
    
    Assert.assertNotEquals(a, b);
  }
  
  @Test
  public void storeIndexSerializationTest() throws Exception {
    @SuppressWarnings("unchecked")
    List<IdentitySet<Index>> indexes = Arrays.<IdentitySet<Index>> asList(IdentitySet.<Index> create(), AscendingIndexIdentitySet.create(), DescendingIndexIdentitySet.create());
    
    Authorizations auths = new Authorizations();
    
    for (Set<Index> index : indexes) {
      Store a = Store.create(dummyConnector, auths, index);

      Value v = PersistedStores.serialize(a);
      Store b = PersistedStores.deserialize(dummyConnector, v);

      Assert.assertEquals("Stores were not equal after serialization when using " + index.getClass(), a, b);
    }
  }
  
  @Test
  public void storeNonSerializedMembersTest() throws Exception {
    Authorizations auths = new Authorizations("a");
    IdentitySet<Index> index = IdentitySet.<Index> create();
    Store a = Store.create(dummyConnector, auths, index);
    
    Value v = PersistedStores.serialize(a);
    Store b = PersistedStores.deserialize(dummyConnector, v);
    
    // Test that we properly recreate the members that we didn't actually serialize
    // into the Accumulo Value
    Assert.assertEquals(a.tracer().getUUID(), b.tracer().getUUID());
    Assert.assertEquals(dummyConnector, b.connector());
  }
  
  @Test
  public void storeEqualityWithAuthsTest() throws Exception {
    Authorizations auths = new Authorizations("a");
    IdentitySet<Index> index = IdentitySet.<Index> create();
    Store a = Store.create(dummyConnector, auths, index);
    
    Value v = PersistedStores.serialize(a);
    Store b = PersistedStores.deserialize(dummyConnector, v);
    
    Assert.assertEquals(a, b);
    
    Store c = Store.create(dummyConnector, new Authorizations("b"), a.uuid(), index);
    
    Assert.assertNotEquals(a, c);
    Assert.assertNotEquals(b, c);
  }
  
  @Test
  public void storeEqualityWithIndexesTest() throws Exception {
    Authorizations auths = new Authorizations();
    Set<Index> index = Sets.newHashSet(Index.define("FOO"));
    Store a = Store.create(dummyConnector, auths, index);
    
    Value v = PersistedStores.serialize(a);
    Store b = PersistedStores.deserialize(dummyConnector, v);
    
    Assert.assertEquals(a, b);
    
    Set<Index> index2 = Sets.newHashSet(Index.define("FOO"));
    
    Store c = Store.create(dummyConnector, auths, a.uuid(), index2);
    
    Assert.assertEquals(a, c);
    Assert.assertEquals(b, c);
  }
  
  @Test
  public void storeEqualityWithIdentityIndexesTest() throws Exception {
    Authorizations auths = new Authorizations();
    IdentitySet<Index> index = IdentitySet.<Index> create();
    Store a = Store.create(dummyConnector, auths, index);
    
    Value v = PersistedStores.serialize(a);
    Store b = PersistedStores.deserialize(dummyConnector, v);
    
    Assert.assertEquals(a, b);
    
    IdentitySet<Index> index2 = IdentitySet.<Index> create();
    
    Store c = Store.create(dummyConnector, auths, a.uuid(), index2);
    
    Assert.assertEquals(a, c);
    Assert.assertEquals(b, c);
    
    Store d = Store.create(dummyConnector, auths, a.uuid(), AscendingIndexIdentitySet.create());
    
    Assert.assertNotEquals(a, d);
    
    Store e = Store.create(dummyConnector, auths, a.uuid(), DescendingIndexIdentitySet.create());
    
    Assert.assertNotEquals(a, e);
    
    Store f = Store.create(dummyConnector, auths, a.uuid(), Sets.<Index> newHashSet());
    
    Assert.assertNotEquals(a, f);
  }
  
  @Test
  public void testEmptyListOfStores() throws Exception {
    Authorizations auths = new Authorizations();
    CloseableIterable<Store> storesIter = PersistedStores.list(mockConnector, auths, Defaults.METADATA_TABLE);
    Assert.assertEquals(0, Iterables.size(storesIter));
  }
  
  @Test
  public void testNonEmptyListOfStores() throws Exception {
    Authorizations auths = new Authorizations();
    
    Store s1 = Store.create(mockConnector, auths, Collections.<Index> emptySet());
    Store s2 = Store.create(mockConnector, auths, Collections.<Index> emptySet());
    Store s3 = Store.create(mockConnector, auths, Collections.<Index> emptySet());
    
    CloseableIterable<Store> storesIter = PersistedStores.list(mockConnector, auths, Defaults.METADATA_TABLE);
    Assert.assertEquals(0, Iterables.size(storesIter));
    
    PersistedStores.store(s1);

    storesIter = PersistedStores.list(mockConnector, auths, Defaults.METADATA_TABLE);
    Assert.assertEquals(1, Iterables.size(storesIter));
    
    PersistedStores.store(s2);
    PersistedStores.store(s3);

    storesIter = PersistedStores.list(mockConnector, auths, Defaults.METADATA_TABLE);
    Assert.assertEquals(3, Iterables.size(storesIter));
  }
  
}
