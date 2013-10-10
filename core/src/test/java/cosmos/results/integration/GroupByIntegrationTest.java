/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 *  Copyright 2013 Josh Elser
 *
 */
package cosmos.results.integration;

import java.io.File;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import cosmos.Cosmos;
import cosmos.IntegrationTests;
import cosmos.impl.CosmosImpl;
import cosmos.options.Defaults;
import cosmos.options.Index;
import cosmos.records.Record;
import cosmos.records.RecordValue;
import cosmos.records.impl.MultimapRecord;
import cosmos.results.CloseableIterable;
import cosmos.results.Column;
import cosmos.store.Store;

/**
 * 
 */
@Category(IntegrationTests.class)
public class GroupByIntegrationTest {
  
  public static MiniAccumuloConfig conf;
  public static MiniAccumuloCluster mac;
  
  protected Cosmos sorts;
  protected ZooKeeperInstance inst;
  protected Connector con;
  
  protected static TestingServer zk;
  
  @BeforeClass
  public static void setupMAC() throws Exception {
    File tmp = Files.createTempDir();
    tmp.deleteOnExit();
    conf = new MiniAccumuloConfig(tmp, "foo");
    conf.setNumTservers(2);
    mac = new MiniAccumuloCluster(conf);
    mac.start();

    
    ZooKeeperInstance zkInst = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
    Connector con = zkInst.getConnector("root", new PasswordToken("foo"));
    con.tableOperations().create(Defaults.DATA_TABLE);
    con.tableOperations().create(Defaults.METADATA_TABLE);

    zk = new TestingServer();
  }
  
  @AfterClass
  public static void stopMAC() throws Exception {
    mac.stop();
  }
  
  @Before
  public void setupSorts() throws Exception {
    sorts = new CosmosImpl(zk.getConnectString());
    inst = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
    con = inst.getConnector("root", new PasswordToken("foo"));
  }
  
  @After
  public void closeSorts() throws Exception {
    sorts.close();
  }
  
  @Test
  public void simpleNonSparseRecordTest() throws Exception {
    Store id = Store.create(con, new Authorizations(), Sets.newHashSet(Index.define("NAME"),
        Index.define("AGE"), Index.define("STATE"), Index.define("COLOR")));
    
    // Register the ID with the implementation
    sorts.register(id);
    
    final ColumnVisibility cv = new ColumnVisibility("");
    
    Multimap<Column,RecordValue> data = HashMultimap.create();
    data.put(Column.create("NAME"), RecordValue.create("Josh", cv));
    data.put(Column.create("AGE"), RecordValue.create("24", cv));
    data.put(Column.create("STATE"), RecordValue.create("MD", cv));
    data.put(Column.create("COLOR"), RecordValue.create("Blue", cv));
    
    MultimapRecord mqr1 = new MultimapRecord(data, "1", cv);
    
    data = HashMultimap.create();
    data.put(Column.create("NAME"), RecordValue.create("Wilhelm", cv));
    data.put(Column.create("AGE"), RecordValue.create("25", cv));
    data.put(Column.create("STATE"), RecordValue.create("MD", cv));
    data.put(Column.create("COLOR"), RecordValue.create("Pink", cv));
    
    MultimapRecord mqr2 = new MultimapRecord(data, "2", cv);
    
    data = HashMultimap.create();
    data.put(Column.create("NAME"), RecordValue.create("Marky", cv));
    data.put(Column.create("AGE"), RecordValue.create("29", cv));
    data.put(Column.create("STATE"), RecordValue.create("MD", cv));
    data.put(Column.create("COLOR"), RecordValue.create("Blue", cv));
    
    MultimapRecord mqr3 = new MultimapRecord(data, "3", cv);
    
    // Add our data
    sorts.addResults(id, Lists.<Record<?>> newArrayList(mqr3, mqr2, mqr1));
    
    // Testing NAME
    Map<RecordValue,Long> expects = ImmutableMap.<RecordValue,Long> of(
        RecordValue.create("Josh", cv), 1l, 
        RecordValue.create("Wilhelm", cv), 1l, 
        RecordValue.create("Marky", cv), 1l);
    
    CloseableIterable<Entry<RecordValue,Long>> countedResults = sorts.groupResults(id, Column.create("NAME"));
    
    int resultCount = 0;
    for (Entry<RecordValue,Long> entry : countedResults) {
      resultCount++;
      
      Assert.assertTrue(expects.containsKey(entry.getKey()));
      Assert.assertEquals(expects.get(entry.getKey()), entry.getValue());
    }
    
    countedResults.close();
    Assert.assertEquals(expects.size(), resultCount);
  
    // TEesting AGE
    expects = ImmutableMap.<RecordValue,Long> of(
        RecordValue.create("24", cv), 1l, 
        RecordValue.create("25", cv), 1l, 
        RecordValue.create("29", cv), 1l);
    
    countedResults = sorts.groupResults(id, Column.create("AGE"));
    
    resultCount = 0;
    for (Entry<RecordValue,Long> entry : countedResults) {
      resultCount++;
      
      Assert.assertTrue(expects.containsKey(entry.getKey()));
      Assert.assertEquals(expects.get(entry.getKey()), entry.getValue());
    }
    
    countedResults.close();
    
    Assert.assertEquals(expects.size(), resultCount);
  
    // Testing STATE
    expects = ImmutableMap.<RecordValue,Long> of(RecordValue.create("MD", cv), 3l);
    
    countedResults = sorts.groupResults(id, Column.create("STATE"));
    
    resultCount = 0;
    for (Entry<RecordValue,Long> entry : countedResults) {
      resultCount++;
      
      Assert.assertTrue(expects.containsKey(entry.getKey()));
      Assert.assertEquals(expects.get(entry.getKey()), entry.getValue());
    }

    countedResults.close();
    
    Assert.assertEquals(expects.size(), resultCount);
  
    // Testing COLOR
    expects = ImmutableMap.<RecordValue,Long> of(
        RecordValue.create("Blue", cv), 2l, 
        RecordValue.create("Pink", cv), 1l);
    
    countedResults = sorts.groupResults(id, Column.create("COLOR"));
    
    resultCount = 0;
    for (Entry<RecordValue,Long> entry : countedResults) {
      resultCount++;
      
      Assert.assertTrue(expects.containsKey(entry.getKey()));
      Assert.assertEquals(expects.get(entry.getKey()), entry.getValue());
    }
    
    countedResults.close();
    
    Assert.assertEquals(expects.size(), resultCount);
  }
  
}
