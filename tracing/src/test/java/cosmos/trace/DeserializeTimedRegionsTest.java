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

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
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
  
  protected DeserializeTimedRegions func;
  
  @Before
  public void setup() {
    func = new DeserializeTimedRegions();
  }
  
  @Test
  public void simpleDeserialize() {
    TimedRegions.Builder regionsBuilder = TimedRegions.newBuilder();
    
    TimedRegion region = TimedRegion.newBuilder().setDescription("desc").setDuration(Long.MAX_VALUE).build();
    
    regionsBuilder.addRegion(region);
    
    TimedRegions regions = regionsBuilder.build();
    
    Value v = new Value(regions.toByteArray());
    
    TimedRegions newRegions = func.apply(Maps.immutableEntry(new Key(), v));
    
    Assert.assertEquals(regions, newRegions);
  }
  
  @Test
  public void emptyValue() {
    TimedRegions empty = TimedRegions.newBuilder().build();
    TimedRegions regions = func.apply(Maps.immutableEntry(new Key(), new Value(new byte[0])));
    
    Assert.assertEquals(empty, regions);
  }
  
  @Test(expected = RuntimeException.class)
  public void invalidValue() {
    func.apply(Maps.immutableEntry(new Key(), new Value(new byte[]{0})));
  }
  
  @Test(expected = RuntimeException.class)
  public void partialProtobuf() {
    TimedRegions.Builder regionsBuilder = TimedRegions.newBuilder();
    TimedRegion region = TimedRegion.newBuilder().setDuration(Long.MAX_VALUE).buildPartial();
    regionsBuilder.addRegion(region);
    
    func.apply(Maps.immutableEntry(new Key(), new Value(regionsBuilder.buildPartial().toByteArray())));
  }
  
}
