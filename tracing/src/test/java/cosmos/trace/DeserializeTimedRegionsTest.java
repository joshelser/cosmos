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

import java.util.Collections;
import java.util.UUID;

import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.ReverseLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

import cosmos.trace.Timings.TimedRegions;
import cosmos.trace.Timings.TimedRegions.TimedRegion;

/**
 * 
 */
public class DeserializeTimedRegionsTest {
  
  protected DeserializeTracer func;
  protected Key k;
  
  @Before
  public void setup() {
    func = new DeserializeTracer();
    LongLexicoder lex = new LongLexicoder();
    ReverseLexicoder<Long> revLex = new ReverseLexicoder<Long>(lex);
    byte[] row = revLex.encode(1l);
    k = new Key(new Text(row), new Text(Tracer.TIME), new Text(UUID.randomUUID().toString()));
  }
  
  @Test
  public void simpleDeserialize() {
    TimedRegion region = TimedRegion.newBuilder().setDescription("desc").setDuration(Long.MAX_VALUE).build();
    
    Tracer tracer = new Tracer(k.getColumnQualifier().toString(), 1l, Collections.singletonList(region));
    
    TimedRegions.Builder regionsBuilder = TimedRegions.newBuilder();
    regionsBuilder.addRegion(region);
    TimedRegions regions = regionsBuilder.build();
    
    // Manually serialize the protobuf
    Value v = new Value(regions.toByteArray());
    
    Tracer newTracer = func.apply(Maps.immutableEntry(k, v));
    
    Assert.assertEquals(tracer, newTracer);
  }
  
  @Test()
  public void emptyValue() {
    TimedRegions empty = TimedRegions.newBuilder().build();
    Tracer tracer = func.apply(Maps.immutableEntry(k, new Value(new byte[0])));
    
    TimedRegions actual = TimedRegions.newBuilder().addAllRegion(tracer.getTimings()).build();
    
    Assert.assertEquals(empty, actual);
  }
  
  @Test(expected = RuntimeException.class)
  public void invalidValue() {
    func.apply(Maps.immutableEntry(k, new Value(new byte[]{0})));
  }
  
  @Test(expected = RuntimeException.class)
  public void partialProtobuf() {
    TimedRegions.Builder regionsBuilder = TimedRegions.newBuilder();
    TimedRegion region = TimedRegion.newBuilder().setDuration(Long.MAX_VALUE).buildPartial();
    regionsBuilder.addRegion(region);
    
    func.apply(Maps.immutableEntry(new Key(), new Value(regionsBuilder.buildPartial().toByteArray())));
  }
  
}
