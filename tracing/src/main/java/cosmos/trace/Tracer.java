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

import org.apache.accumulo.core.client.lexicoder.IntegerLexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.ReverseLexicoder;
import org.apache.accumulo.core.data.Mutation;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
  
  public void addTiming(String description, Stopwatch sw) {
    checkNotNull(description);
    checkNotNull(sw);
    
    if (-1l == begin) {
      begin = System.currentTimeMillis();
    }
    
    this.timings.add(Maps.immutableEntry(description, sw));
  }
  
  public List<Mutation> toMutations() {
    if (this.timings.isEmpty()) {
      return Collections.emptyList();
    }
    
    Mutation recordMutation = new Mutation(uuid);
    
    IntegerLexicoder intLex = new IntegerLexicoder();
    LongLexicoder longLex = new LongLexicoder();
    ReverseLexicoder<Long> revLongLex = new ReverseLexicoder<Long>(longLex);
    
    long duration = 0;
    for (int i = 0; i < this.timings.size(); i++) {
      String description = this.timings.get(i).getKey();
      Stopwatch timing = this.timings.get(i).getValue();
      long millis = timing.elapsed(TimeUnit.MILLISECONDS);
      duration += millis;
      recordMutation.put(UUID.getBytes(), intLex.encode(i), longLex.encode(millis));
    }
    
    Mutation timeMutation = new Mutation(revLongLex.encode(begin));
    timeMutation.put(TIME.getBytes(), this.uuid.getBytes(), longLex.encode(duration));
    
    return Arrays.asList(recordMutation, timeMutation);
  }
  
}
