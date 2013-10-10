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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mediawiki.xml.export_0.MediaWikiType;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import cosmos.Cosmos;
import cosmos.IntegrationTests;
import cosmos.impl.CosmosImpl;
import cosmos.options.Defaults;
import cosmos.options.Index;
import cosmos.results.CloseableIterable;
import cosmos.results.Column;
import cosmos.results.Record;
import cosmos.results.RecordValue;
import cosmos.results.impl.MultimapRecord;
import cosmos.store.Store;

/**
 * 
 */
@Category(IntegrationTests.class)
public class CosmosIntegrationTest extends CosmosIntegrationSetup {
  protected static MiniAccumuloCluster mac;
  protected static File macDir;
  protected static TestingServer zk;
  
  @BeforeClass
  public static void createAccumuloCluster() throws Exception {
    macDir = File.createTempFile("miniaccumulocluster", null);
    Assert.assertTrue(macDir.delete());
    Assert.assertTrue(macDir.mkdir());
    macDir.deleteOnExit();
    
    MiniAccumuloConfig config = new MiniAccumuloConfig(macDir, "");
    config.setNumTservers(2);
    
    mac = new MiniAccumuloCluster(config);
    mac.start();
    
    ZooKeeperInstance zkInst = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
    Connector c = zkInst.getConnector("root", new PasswordToken(""));
    
    // Add in auths for "en"
    c.securityOperations().changeUserAuthorizations("root", new Authorizations("en"));
    
    zk = new TestingServer();
  }
  
  @AfterClass
  public static void stopAccumuloCluster() throws Exception {
    mac.stop();
    FileUtils.deleteDirectory(macDir);
  }
  
  @Test
  public void test() throws Exception {
    // Cache all of the wikis -- multithreaded
    loadAllWikis();
    
    long start = System.currentTimeMillis();
    
    // These should all be cached
    Assert.assertNotNull(getWiki1());
    Assert.assertNotNull(getWiki2());
    Assert.assertNotNull(getWiki3());
    Assert.assertNotNull(getWiki4());
    Assert.assertNotNull(getWiki5());
    
    long end = System.currentTimeMillis();
    
    Assert.assertTrue((end - start) < 10000);
  }
  
  @Test
  public void testWiki1() throws Exception {
    // Get the same wiki 3 times
    List<Thread> threads = Lists.newArrayList();
    final List<Long> timings = Lists.newArrayList();
    
    // Preload
    Stopwatch sw = new Stopwatch();
    sw.start();
    getWiki1();
    sw.stop();
    long origLength = sw.elapsed(TimeUnit.MICROSECONDS);
    
    for (int i = 0; i < 3; i++) {
      threads.add(new Thread(new Runnable() {
        public void run() {
          try {
            Stopwatch sw = new Stopwatch();
            sw.start();
            getWiki1();
            sw.stop();
            timings.add(sw.elapsed(TimeUnit.MICROSECONDS));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }));
    }
    
    long start = System.currentTimeMillis();
    
    for (Thread t : threads) {
      t.start();
    }
    
    for (Thread t : threads) {
      t.join();
    }
    
    long end = System.currentTimeMillis();
    
    for (Long duration : timings) {
      Assert.assertTrue(origLength > duration);
    }
  }
  
  @Test
  public void wiki1Test() throws Exception {
    MediaWikiType wiki1 = getWiki1();
    List<Record<?>> results = wikiToMultimap(wiki1);
    
    ZooKeeperInstance zk = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
    Connector con = zk.getConnector("root", new PasswordToken(""));
    con.tableOperations().create(Defaults.DATA_TABLE);
    con.tableOperations().create(Defaults.METADATA_TABLE);
    
    Store id = Store.create(con, new Authorizations("en"),
        Sets.newHashSet(Index.define(PAGE_ID)));
    
    Cosmos s = new CosmosImpl(CosmosIntegrationTest.zk.getConnectString());
    
    s.register(id);
    s.addResults(id, results);
    
    Column pageIdCol = Column.create(PAGE_ID);
    
    CloseableIterable<MultimapRecord> newResults = s.fetch(id);
    
    Assert.assertNotNull(newResults);
    
    long count = 0;
    String prevPageId = "";
    for (MultimapRecord res : newResults) {
      Collection<RecordValue> pageIds = res.get(pageIdCol);
      String currPageId = null;
      for (RecordValue pageId : pageIds) {
        if (null == currPageId) {
          currPageId = pageId.value();
        }

        // If we have multiple pageIds for this record, choose the least one greater than the prev
        if (prevPageId.compareTo(pageId.value()) < 0 && pageId.value().compareTo(currPageId) < 0) {
          currPageId = pageId.value();
        }
      }
      count++;
    }
    
    Assert.assertEquals(wiki1.getPage().size(), count);
    
    newResults.close();
    s.close();
  }
  
}
