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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.lexicoder.ReverseLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Lists;

import cosmos.accumulo.GroupByRowSuffixIterator;
import cosmos.accumulo.OrderFilter;
import cosmos.impl.GroupByFunction;
import cosmos.options.Defaults;
import cosmos.options.Order;
import cosmos.results.CloseableIterable;
import cosmos.results.SValue;


/**
 * 
 */
public class CosmosTest {
  public static void foo(String[] args) throws Exception {
    List<String> data = Lists.newArrayList("aaa", "aab");
    
    StringLexicoder lex = new StringLexicoder();
    ReverseLexicoder<String> revlex = new ReverseLexicoder<String>(lex);
    
    byte[] b1 = lex.encode(data.get(0)), b2 = lex.encode(data.get(1));
    
    System.out.println(new String(b1));
    System.out.println(new String(b2));
    
    b1 = revlex.encode(data.get(0));
    b2 = revlex.encode(data.get(1));

    List<String> revData = Lists.newArrayList(new String(b1), new String(b2));
    Collections.sort(revData);
    for (String s : revData) {
      System.out.println(s);
      System.out.println(Arrays.toString(s.getBytes()));
    }
    
    System.exit(1);
  }
  
  public static void main(String[] args) throws Exception {
    foo(args);
    ZooKeeperInstance zk = new ZooKeeperInstance("accumulo1.5", "localhost");
    Connector con = zk.getConnector("mediawiki", new PasswordToken("password"));

    BatchScanner bs = con.createBatchScanner(Defaults.DATA_TABLE, con.securityOperations().getUserAuthorizations("mediawiki"), 10);
    bs.setRanges(Collections.singleton(Range.prefix("55b672e0-a5c8-4ff7-81a9-bfd7e52cde74")));
    bs.fetchColumnFamily(new Text("CONTRIBUTOR_USERNAME"));


    // TODO Need to post filter on cq-prefix to only look at the ordering we want
    IteratorSetting filter = new IteratorSetting(50, "cqFilter", OrderFilter.class);
    filter.addOption(OrderFilter.PREFIX, Order.FORWARD);
    bs.addScanIterator(filter);
    
    IteratorSetting cfg = new IteratorSetting(60, GroupByRowSuffixIterator.class);
    bs.addScanIterator(cfg);
    
    CloseableIterable<Entry<SValue,Long>> iter = CloseableIterable.transform(bs, new GroupByFunction());
    for (Entry<SValue,Long> entry : iter) {
      System.out.println(entry.getKey().value() + "=" + entry.getValue());
    }
    
    bs.close();
  }
}
