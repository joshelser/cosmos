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

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.ReverseLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import cosmos.trace.Timings.TimedRegions;

/**
 * 
 */
public class TracerClient {
  private static final Logger log = LoggerFactory.getLogger(TracerClient.class);
  
  protected final Connector connector;
  
  public TracerClient(Connector c) {
    checkNotNull(c);
    
    this.connector = c;
  }
  
  public TimedRegions getTimings(String uuid) throws NoSuchElementException {
    checkNotNull(uuid);
    
    Scanner scanner;
    try {
      scanner = connector.createScanner(AccumuloTraceStore.TABLE_NAME, new Authorizations());
    } catch (TableNotFoundException e) {
      log.error("Couldn't find the traces table {}", AccumuloTraceStore.TABLE_NAME);
      throw new NoSuchElementException("No trace found for " + uuid);
    }
    scanner.fetchColumnFamily(new Text(Tracer.UUID));
    scanner.setRange(Range.exact(uuid));
    
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    if (iter.hasNext()) {
      return DeserializeTimedRegions.deserialize(iter.next());
    }
    
    throw new NoSuchElementException("No trace found for " + uuid);
  }
  
  public List<TimedRegions> mostRecentTimings(int numberOfTimings) {
    Scanner scanner;
    
    try {
      scanner = connector.createScanner(AccumuloTraceStore.TABLE_NAME, new Authorizations());
    } catch (TableNotFoundException e) {
      log.error("Couldn't find the traces table {}", AccumuloTraceStore.TABLE_NAME);
      throw new RuntimeException(e);
    }
    
    scanner.fetchColumnFamily(new Text(Tracer.TIME));
    scanner.setRange(new Range());
    
    List<TimedRegions> timings = Lists.newArrayListWithCapacity(numberOfTimings);
    for (Entry<Key,Value> entry : scanner) {
      timings.add(DeserializeTimedRegions.deserialize(entry));
    }
    
    return timings;
  }
  
  public Iterable<TimedRegions> since(Date end) {
    checkNotNull(end);
    
    return since(end.getTime());
  }
  
  public Iterable<TimedRegions> since(long timeInMillis) {
    Preconditions.checkArgument(System.currentTimeMillis() > timeInMillis, "Cannot find traces in the future");
    Preconditions.checkArgument(0 < timeInMillis, "Must have a positive begin");
    
    Scanner scanner;
    
    LongLexicoder longLex = new LongLexicoder();
    ReverseLexicoder<Long> revLongLex = new ReverseLexicoder<Long>(longLex);
    
    Text encodedSince = new Text(revLongLex.encode(timeInMillis));
    
    try {
      scanner = connector.createScanner(AccumuloTraceStore.TABLE_NAME, new Authorizations());
    } catch (TableNotFoundException e) {
      log.error("Couldn't find the traces table {}", AccumuloTraceStore.TABLE_NAME);
      throw new RuntimeException(e);
    }
    
    scanner.fetchColumnFamily(new Text(Tracer.TIME));
    scanner.setRange(new Range(null, false, encodedSince, true));
    
    return Iterables.transform(scanner, new DeserializeTimedRegions());
  }
  
  public Iterable<TimedRegions> between(Date start, Date end) {
    checkNotNull(start);
    checkNotNull(end);
    
    return between(start.getTime(), end.getTime());
  }
  
  public Iterable<TimedRegions> between(Long start, Long end) {
    Preconditions.checkArgument(System.currentTimeMillis() > start, "Cannot find traces in the future");
    Preconditions.checkArgument(0 < start, "Must have a positive begin");
    Preconditions.checkArgument(start <= end, "Start must be less than end");
    
    Scanner scanner;
    
    LongLexicoder longLex = new LongLexicoder();
    ReverseLexicoder<Long> revLongLex = new ReverseLexicoder<Long>(longLex);
    
    Text encodedStart = null != start ? new Text(revLongLex.encode(start)) : null;
    Text encodedEnd = null != end ? new Text(revLongLex.encode(end)) : null;
    
    try {
      scanner = connector.createScanner(AccumuloTraceStore.TABLE_NAME, new Authorizations());
    } catch (TableNotFoundException e) {
      log.error("Couldn't find the traces table {}", AccumuloTraceStore.TABLE_NAME);
      throw new RuntimeException(e);
    }
    
    scanner.fetchColumnFamily(new Text(Tracer.TIME));
    
    // Since the start and end are also reverse encoded, we use them as normal
    scanner.setRange(new Range(encodedEnd, false, encodedStart, false));
    
    return Iterables.transform(scanner, new DeserializeTimedRegions());
  }
}
