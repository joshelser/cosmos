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

package cosmos;

import java.util.Collections;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Range;

import cosmos.impl.CosmosImpl;
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
    Connector con = zk.getConnector("mediawiki", "password");

    BatchDeleter bd = con.createBatchDeleter(Defaults.DATA_TABLE, con.securityOperations().getUserAuthorizations("mediawiki"), 10, CosmosImpl.DEFAULT_MAX_MEMORY, CosmosImpl.DEFAULT_MAX_LATENCY,
        CosmosImpl.DEFAULT_MAX_WRITE_THREADS);
    
    bd.setRanges(Collections.singleton(new Range()));
    bd.delete();
    
    bd.close();
    
    con.tableOperations().compact(Defaults.DATA_TABLE, null, null, true, true);
    
    bd = con.createBatchDeleter(Defaults.METADATA_TABLE, con.securityOperations().getUserAuthorizations("mediawiki"), 10, CosmosImpl.DEFAULT_MAX_MEMORY, CosmosImpl.DEFAULT_MAX_LATENCY,
        CosmosImpl.DEFAULT_MAX_WRITE_THREADS);
    
    bd.setRanges(Collections.singleton(new Range()));
    bd.delete();
    
    bd.close();
    
    con.tableOperations().compact(Defaults.METADATA_TABLE, null, null, true, true);
  }
  
}
