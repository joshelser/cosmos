/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cosmos.example;

import java.io.File;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import cosmos.Cosmos;
import cosmos.impl.CosmosImpl;
import cosmos.options.Index;
import cosmos.records.values.RecordValue;
import cosmos.results.CloseableIterable;
import cosmos.results.Column;
import cosmos.store.Store;
import cosmos.util.AscendingIndexIdentitySet;

/**
 * 
 */
public class BuildingPermitsExample {
  private static final Logger log = LoggerFactory.getLogger(BuildingPermitsExample.class);
  
  @Parameter(names = { "--zookeepers", "-z"}, description = "CSV of zookeepers to use")
  public String zookeepers;
  
  @Parameter(names = {"--instance", "-i"}, description = "Accumulo instance name")
  public String instanceName;
  
  @Parameter(names = {"--username", "-u"}, description = "Accumulo user name")
  public String username;
  
  @Parameter(names = {"--password", "-p"}, description = "Accumulo user password")
  public String password;
  
  @Parameter(names = {"--miniAccumuloCluster", "-mac"}, description = "Use a MiniAccumuloCluster instead of a real instance")
  public boolean useMiniAccumuloCluster = false;
  
  @Parameter(names = {"--input-file", "-f"}, description = "Path to the CSV file to load", required = true)
  public String fileName;
  
  public BuildingPermitsExample() { }
  
  
  public static void main(String[] args) throws Exception {
    BuildingPermitsExample example = new BuildingPermitsExample();
    new JCommander(example, args);
  
    File inputFile = new File(example.fileName);
    
    Preconditions.checkArgument(inputFile.exists() && inputFile.isFile() && inputFile.canRead(), 
        "Expected " + example.fileName + " to be a readable file");
    
    String zookeepers;
    String instanceName;
    Connector connector;
    MiniAccumuloCluster mac = null;
    File macDir = null;
    
    // Use the MiniAccumuloCluster is requested
    if (example.useMiniAccumuloCluster) {
      macDir = Files.createTempDir();
      String password = "password";
      MiniAccumuloConfig config = new MiniAccumuloConfig(macDir, password);
      config.setNumTservers(1);
      
      mac = new MiniAccumuloCluster(config);
      mac.start();
      
      zookeepers = mac.getZooKeepers();
      instanceName = mac.getInstanceName();
      
      ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zookeepers);
      connector = instance.getConnector("root", new PasswordToken(password));
    } else {
      // Otherwise connect to a running instance
      zookeepers = example.zookeepers;
      instanceName = example.instanceName;
      
      ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zookeepers);
      connector = instance.getConnector(example.username, new PasswordToken(example.password));
    }
    
    // Instantiate an instance of Cosmos
    Cosmos cosmos = new CosmosImpl(zookeepers);
    
    // Create a definition for the data we want to load
    Store id = Store.create(connector, new Authorizations(), AscendingIndexIdentitySet.create());
    
    // Register the definition with Cosmos so it can track its progress.
    cosmos.register(id);
    
    // Load all of the data from our inputFile
    LoadBuildingPermits loader = new LoadBuildingPermits(cosmos, id, inputFile);
    loader.run();
    
    // Finalize the SortableResult which will prevent future writes to the data set
    cosmos.finalize(id);
    
    // Flush the ingest traces to the backend so we can see the results;
    id.sendTraces();
    
    // Get back the Set of Columns that we've ingested.
    Set<Column> schema = Sets.newHashSet(cosmos.columns(id));
    
    log.debug("\nColumns: " + schema);
    
    Iterator<Column> iter = schema.iterator();
    while (iter.hasNext()) {
      Column c = iter.next();
      // Remove the internal ID field and columns that begin with CONTRACTOR_
      if (c.equals(LoadBuildingPermits.ID) || c.name().startsWith("CONTRACTOR_")) {
        iter.remove();
      }
    }
    
    Iterable<Index> indices = Iterables.transform(schema, new Function<Column,Index>() {

      @Override
      public Index apply(Column col) {
        return Index.define(col);
      }
      
    });
    
    // Ensure that we have locality groups set as we expect
    log.info("Ensure locality groups are set");
    id.optimizeIndices(indices);
    
    // Compact down the data for this SortableResult    
    log.info("Issuing compaction for relevant data");
    id.consolidate();
    
    final int numTopValues = 10;
    
    // Walk through each column in the result set
    for (Column c : schema) {
      Stopwatch sw = new Stopwatch();
      sw.start();
      
      // Get the number of times we've seen each value in a given column
      CloseableIterable<Entry<RecordValue<?>,Long>> groupingsInColumn = cosmos.groupResults(id, c);
      
      log.info(c.name() + ":");
      
      // Iterate over the counts, collecting the top N values in each column
      TreeMap<Long,RecordValue<?>> topValues = Maps.newTreeMap();
      
      for (Entry<RecordValue<?>,Long> entry : groupingsInColumn) {
        if (topValues.size() == numTopValues) {
          Entry<Long,RecordValue<?>> least = topValues.pollFirstEntry();
          
          if (least.getKey() < entry.getValue()) {
            topValues.put(entry.getValue(), entry.getKey());
          } else {
            topValues.put(least.getKey(), least.getValue());
          }
        } else if (topValues.size() < numTopValues) {
          topValues.put(entry.getValue(), entry.getKey());
        }
      }
      
      for (Long key: topValues.descendingKeySet()) {
        log.info(topValues.get(key).value() + " occurred " + key + " times");
      }

      sw.stop();
      
      log.info("Took " + sw.toString() + " to run query.\n");
    }
    
    log.info("Deleting records");
    
    // Delete the records we've ingested
    if (!example.useMiniAccumuloCluster) {
      // Because I'm lazy and don't want to wait around to run the BatchDeleter when we're just going
      // to rm -rf the directory in a few secs.
      cosmos.delete(id);
    }
    
    // And shut down Cosmos
    cosmos.close();
    
    log.info("Cosmos stopped");
    
    // If we were using MAC, also stop that
    if (example.useMiniAccumuloCluster && null != mac) {
      mac.stop();
      if (null != macDir) {
        FileUtils.deleteDirectory(macDir);
      }
    }
  }
}
