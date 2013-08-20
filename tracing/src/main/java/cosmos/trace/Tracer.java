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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.ReverseLexicoder;
import org.apache.accumulo.core.data.Mutation;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import cosmos.trace.Timings.TimedRegions;
import cosmos.trace.Timings.TimedRegions.TimedRegion;

/**
 * 
 */
public class Tracer {
  public static final String UUID = "uuid", TIME = "time";
  
  protected final String uuid;
  protected long begin = -1l;
  protected final List<Entry<String,Stopwatch>> timings;
  
  public Tracer(String uuid) {
    checkNotNull(uuid);
    
    this.uuid = uuid;
    this.timings = Lists.newArrayList();
  }
  
  public Stopwatch startTiming(String description) {
    checkNotNull(description);
    
    if (-1l == begin) {
      begin = System.currentTimeMillis();
    }
    
    Stopwatch sw = new Stopwatch();
    this.timings.add(Maps.immutableEntry(description, sw));
    sw.start();
    
    return sw;
  }
  
  public List<Mutation> toMutations() {
    Preconditions.checkArgument(-1 != begin, "Found `begin` still equal to -1");
    
    if (this.timings.isEmpty()) {
      return Collections.emptyList();
    }
    
    Mutation recordMutation = new Mutation(uuid);
    
    LongLexicoder longLex = new LongLexicoder();
    ReverseLexicoder<Long> revLongLex = new ReverseLexicoder<Long>(longLex);
    
    TimedRegions.Builder builder = TimedRegions.newBuilder();
    for (int i = 0; i < this.timings.size(); i++) {
      String description = this.timings.get(i).getKey();
      Stopwatch timing = this.timings.get(i).getValue();
      
      Preconditions.checkArgument(!timing.isRunning(), "Found a non-stopped Stopwatch for region " + description);
      
      long millis = timing.elapsed(TimeUnit.MILLISECONDS);
      
      TimedRegion region = TimedRegion.newBuilder().setDuration(millis).setDescription(description).build();
      builder.addRegion(region); 
    }
    
    byte[] serializedBytes = builder.build().toByteArray();
    recordMutation.put(UUID.getBytes(), new byte[0], serializedBytes);
    
    System.out.println("begin:" + begin);
    Mutation timeMutation = new Mutation(revLongLex.encode(begin));
    timeMutation.put(TIME.getBytes(), this.uuid.getBytes(), serializedBytes);
    
    return Arrays.asList(recordMutation, timeMutation);
  }
  
}
