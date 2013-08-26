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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.ReverseLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;

import cosmos.trace.Timings.TimedRegions;
import cosmos.trace.Timings.TimedRegions.TimedRegion;
import cosmos.trace.Timings.TimedRegions.TimingMetadata;

/**
 * 
 */
public class Tracer {
  public static final String UUID = "uuid", TIME = "time";
  public static final Text UUID_TEXT = new Text(UUID), TIME_TEXT = new Text(TIME);
  
  protected final String uuid;
  protected long begin;
  protected final List<TimedRegion> timings;
  protected Map<String,String> metadata;
  
  public Tracer(String uuid) {
    checkNotNull(uuid);
    
    this.uuid = uuid;
    this.begin = System.currentTimeMillis();
    this.timings = Lists.newArrayList();
    this.metadata = Maps.newHashMap();
  }
  
  public Tracer(Tracer other) {
    checkNotNull(other);
    
    this.uuid = other.uuid;
    this.begin = other.begin;
    this.timings = Lists.newArrayList(other.timings);
    this.metadata = Maps.newHashMap(other.metadata);
  }
  
  public Tracer(String uuid, long begin, List<TimedRegion> timings) {
    checkNotNull(uuid);
    checkNotNull(begin);
    checkNotNull(timings);
    
    this.uuid = uuid;
    this.begin = begin;
    this.timings = timings;
    this.metadata = Maps.newHashMap();
  }
  
  public Tracer(String uuid, long begin, List<TimedRegion> timings, Map<String,String> metadata) {
    checkNotNull(uuid);
    checkNotNull(begin);
    checkNotNull(timings);
    checkNotNull(metadata);
    
    this.uuid = uuid;
    this.begin = begin;
    this.timings = timings;
    this.metadata = Maps.newHashMap(metadata);
  }

  public Tracer(Entry<Key,Value> entry) {
    this(entry.getValue());
  }
  
  public Tracer(Value timeValue) {
    checkNotNull(timeValue);
    
    TimedRegions regions = null;
    try {
      regions = TimedRegions.parseFrom(timeValue.get());  
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    
    this.uuid = regions.getUuid();
    this.begin = regions.getBegin();
    this.timings = regions.getRegionList();
    this.metadata = Maps.newHashMap();
    for (TimingMetadata metadata : regions.getMetadataList()) {
      this.metadata.put(metadata.getName(), metadata.getValue());
    }
  }
  
  public void addTiming(TimedRegion timing) {
    this.timings.add(timing);
  }
  
  public void addTiming(String description, Long duration) {
    checkNotNull(description);
    checkArgument(null != duration && -1 < duration, "Duration must be non-null and non-negative");

    addTiming(TimedRegion.newBuilder().setDescription(description).setDuration(duration).build());
  }
  
  public String getUUID() {
    return this.uuid; 
  }
  
  public long getBegin() {
    return this.begin;
  }
  
  public List<TimedRegion> getTimings() {
    return Collections.unmodifiableList(timings);
  }
  
  public List<Mutation> toMutations() {
    if (this.timings.isEmpty()) {
      return Collections.emptyList();
    }
    
    Mutation recordMutation = new Mutation(uuid);
    
    LongLexicoder longLex = new LongLexicoder();
    ReverseLexicoder<Long> revLongLex = new ReverseLexicoder<Long>(longLex);
    
    TimedRegions.Builder builder = TimedRegions.newBuilder();
    builder.addAllRegion(this.timings);
    
    builder.setBegin(this.begin);
    builder.setUuid(this.uuid);
    
    for (Entry<String,String> entry : this.metadata.entrySet()) {
      builder.addMetadata(TimingMetadata.newBuilder().setName(entry.getKey()).setValue(entry.getValue()).build());
    }
    
    byte[] serializedBytes = builder.build().toByteArray();
    recordMutation.put(UUID, "", new Value(serializedBytes));
    
    System.out.println("begin:" + begin);
    Mutation timeMutation = new Mutation(new Text(revLongLex.encode(begin)));
    timeMutation.put(TIME, this.uuid, new Value(serializedBytes));
    
    return Arrays.asList(recordMutation, timeMutation);
  }
 
  public boolean equals(Object o) {
    if (null == o) {
      return false;
    }
    
    if (o instanceof Tracer) {
      Tracer other = (Tracer) o;
      
      return this.uuid.equals(other.uuid) && this.begin == other.begin && this.timings.equals(other.timings); 
    }
    
    return false;
  }
  
}
