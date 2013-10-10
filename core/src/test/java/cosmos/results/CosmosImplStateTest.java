package cosmos.results;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.HashMultimap;

import cosmos.Cosmos;
import cosmos.UnexpectedStateException;
import cosmos.impl.CosmosImpl;
import cosmos.options.Index;
import cosmos.options.Paging;
import cosmos.records.RecordValue;
import cosmos.records.impl.MultimapRecord;
import cosmos.store.Store;
import cosmos.util.IdentitySet;

public class CosmosImplStateTest extends AbstractSortableTest {
  
  @Test
  public void testDuplicateRegister() throws Exception {
    Store id = Store.create(c, AUTHS, IdentitySet.<Index> create());
    Cosmos cosmos = new CosmosImpl(zkConnectString());
    
    cosmos.register(id);
    
    try {
      cosmos.register(id);
    } catch (UnexpectedStateException e) {
      return;
    } finally {
      cosmos.close();
    }
    
    Assert.fail("Expected exception to be throw after duplicate 'register' of Store");
  }
  
  @Test(expected = UnexpectedStateException.class)
  public void testNonRegisteredStoreAddingResult() throws Exception {
    Store id = Store.create(c, AUTHS, IdentitySet.<Index> create());
    Cosmos cosmos = new CosmosImpl(zkConnectString());
    
    MultimapRecord mqr = new MultimapRecord(HashMultimap.<Column,RecordValue> create(), "1", VIZ);
    
    cosmos.addResult(id, mqr);
  }
  
  @Test(expected = UnexpectedStateException.class)
  public void testNonRegisteredStoreAddingResults() throws Exception {
    Store id = Store.create(c, AUTHS, IdentitySet.<Index> create());
    Cosmos cosmos = new CosmosImpl(zkConnectString());
    
    MultimapRecord mqr = new MultimapRecord(HashMultimap.<Column,RecordValue> create(), "1", VIZ);
    
    cosmos.addResults(id, Collections.singleton(mqr));
  }
  
  @Test(expected = UnexpectedStateException.class)
  public void testNonRegisteredIndexCall() throws Exception {
    Store id = Store.create(c, AUTHS, IdentitySet.<Index> create());
    Cosmos cosmos = new CosmosImpl(zkConnectString());
    
    cosmos.index(id, IdentitySet.<Index> create());
  }
  
  @Test(expected = UnexpectedStateException.class)
  public void testNonRegisteredColumnsCall() throws Exception {
    Store id = Store.create(c, AUTHS, IdentitySet.<Index> create());
    Cosmos cosmos = new CosmosImpl(zkConnectString());
    
    cosmos.columns(id);
  }
  
  @Test(expected = UnexpectedStateException.class)
  public void testNonRegisteredFetchCall() throws Exception {
    Store id = Store.create(c, AUTHS, IdentitySet.<Index> create());
    Cosmos cosmos = new CosmosImpl(zkConnectString());
    
    cosmos.fetch(id);
  }
  
  @Test(expected = UnexpectedStateException.class)
  public void testNonRegisteredFetchWithColumnValueCall() throws Exception {
    Store id = Store.create(c, AUTHS, IdentitySet.<Index> create());
    Cosmos cosmos = new CosmosImpl(zkConnectString());
    
    cosmos.fetch(id, Column.create("FOO"), "Bar");
  }
  
  @Test(expected = UnexpectedStateException.class)
  public void testNonRegisteredFetchWithColumnValueAndPagingCall() throws Exception {
    Store id = Store.create(c, AUTHS, IdentitySet.<Index> create());
    Cosmos cosmos = new CosmosImpl(zkConnectString());
    
    cosmos.fetch(id, Column.create("FOO"), "Bar", Paging.create(Integer.MAX_VALUE, Integer.MAX_VALUE));
  }
  
  @Test(expected = UnexpectedStateException.class)
  public void testNonRegisteredFetchWithOrderCall() throws Exception {
    Store id = Store.create(c, AUTHS, IdentitySet.<Index> create());
    Cosmos cosmos = new CosmosImpl(zkConnectString());
    
    cosmos.fetch(id, Index.define("FOO"));
  }
  
  @Test(expected = UnexpectedStateException.class)
  public void testNonRegisteredContentsCall() throws Exception {
    Store id = Store.create(c, AUTHS, IdentitySet.<Index> create());
    Cosmos cosmos = new CosmosImpl(zkConnectString());
    
    cosmos.contents(id, "1");
  }
  
  @Test(expected = UnexpectedStateException.class)
  public void testNonRegisteredDeleteCall() throws Exception {
    Store id = Store.create(c, AUTHS, IdentitySet.<Index> create());
    Cosmos cosmos = new CosmosImpl(zkConnectString());
    
    cosmos.delete(id);
  }
  
}
