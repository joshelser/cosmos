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
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;


/**
 * 
 */
public class FixRowKey {

  public static void main(String[] args) throws Exception {
//    System.out.println(String.format("%011d", Integer.MAX_VALUE));
    ZooKeeperInstance inst = new ZooKeeperInstance("accumulo1.5", "localhost");
    Connector c = inst.getConnector("root", new PasswordToken("secret"));
    
    BatchScanner bs = c.createBatchScanner("sortswiki", c.securityOperations().getUserAuthorizations(c.whoami()), 2);
    BatchWriter bw = c.createBatchWriter("sortswiki2", new BatchWriterConfig().setMaxMemory(100*1024*1024).setMaxWriteThreads(6));
    
    bs.setRanges(Collections.singleton(new Range()));
    
    final Text holder = new Text();
    final byte[] empty = new byte[0];
    for (Entry<Key,Value> e : bs) {
      e.getKey().getRow(holder);
      Integer i = Integer.parseInt(holder.toString());
      Mutation m = new Mutation(String.format("%011d", i));
      m.put(empty, empty, e.getValue().get());
      bw.addMutation(m);
    }
    
    bs.close();
    bw.close();
  }
}
