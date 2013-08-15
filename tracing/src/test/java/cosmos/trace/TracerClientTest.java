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
package cosmos.trace;

import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.ReverseLexicoder;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

import cosmos.trace.Timings.TimedRegions;

/**
 * 
 */
public class TracerClientTest {
  
  private static MockInstance instance;
  private static Connector con;
  
  @BeforeClass
  public static void initializeMockAccumulo() throws Exception {
    instance = new MockInstance(TracerClientTest.class.getName());
    con = instance.getConnector("foo", new PasswordToken("bar"));
    
    AccumuloTraceStore.ensureTables(con);
  }
  
  @Test
  public void singleSince() throws Exception {
    Tracer t1 = new Tracer("1");
    t1.begin = 10l;
    
    Stopwatch sw1 = t1.startTiming("t1");
    
    sw1.stop();
    
    AccumuloTraceStore.serialize(t1, con);
    
    TracerClient tc = new TracerClient(con);
    
    List<TimedRegions> regions = Lists.newArrayList(tc.since(11));
    
    Assert.assertEquals(0, regions.size());
    
    regions = Lists.newArrayList(tc.since(9));
    
    Assert.assertEquals(1, regions.size());
  }
  
  @Test
  public void multipleSince() throws Exception {
    Tracer t1 = new Tracer("1");
    t1.begin = 10l;
    Tracer t2 = new Tracer("2");
    t2.begin = 20l;
    
    Stopwatch sw1 = t1.startTiming("t1");
    Stopwatch sw2 = t2.startTiming("t2");
    
    sw1.stop();
    sw2.stop();
    
    AccumuloTraceStore.serialize(t1, con);
    AccumuloTraceStore.serialize(t2, con);
    
    TracerClient tc = new TracerClient(con);
    
    List<TimedRegions> regions = Lists.newArrayList(tc.since(11));
    
    Assert.assertEquals(1, regions.size());
    
    TimedRegions trs = regions.get(0);
    Assert.assertEquals(1, trs.getRegionCount());
    Assert.assertEquals("t2", trs.getRegion(0).getDescription());
    
    regions = Lists.newArrayList(tc.since(9));
    
    Assert.assertEquals(2, regions.size());
    
    regions = Lists.newArrayList(tc.since(21));
    
    Assert.assertEquals(0, regions.size());
  }
  
  @Test
  public void multipleBetween() throws Exception {
    Tracer t1 = new Tracer("1");
    t1.begin = 10l;
    Tracer t2 = new Tracer("2");
    t2.begin = 20l;
    
    Stopwatch sw1 = t1.startTiming("t1");
    Stopwatch sw2 = t2.startTiming("t2");
    
    sw1.stop();
    sw2.stop();
    
    AccumuloTraceStore.serialize(t1, con);
    AccumuloTraceStore.serialize(t2, con);
    
    TracerClient tc = new TracerClient(con);
    
    List<TimedRegions> regions = Lists.newArrayList(tc.between(19l, 21l));
    
    Assert.assertEquals(1, regions.size());
    
    TimedRegions trs = regions.get(0);
    Assert.assertEquals(1, trs.getRegionCount());
    Assert.assertEquals("t2", trs.getRegion(0).getDescription());
    
    regions = Lists.newArrayList(tc.between(21l, 23l));
    Assert.assertEquals(0, regions.size());

    regions = Lists.newArrayList(tc.between(13l, 18l));
    Assert.assertEquals(0, regions.size());

    regions = Lists.newArrayList(tc.between(2l, 4l));
    Assert.assertEquals(0, regions.size());
    
    regions = Lists.newArrayList(tc.between(8l, 12l));
    
    Assert.assertEquals(1, regions.size());
    
    trs = regions.get(0);
    Assert.assertEquals(1, trs.getRegionCount());
    Assert.assertEquals("t1", trs.getRegion(0).getDescription());
    
    regions = Lists.newArrayList(tc.between(8l, 22l));
    
    Assert.assertEquals(2, regions.size());
    
    trs = regions.get(0);
    Assert.assertEquals(1, trs.getRegionCount());
    Assert.assertEquals("t2", trs.getRegion(0).getDescription());
    
    trs = regions.get(1);
    Assert.assertEquals(1, trs.getRegionCount());
    Assert.assertEquals("t1", trs.getRegion(0).getDescription());
  }
  
}
