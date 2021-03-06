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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.ReverseLexicoder;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
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

/**
 * 
 */
public class TracerClient {
  private static final Logger log = LoggerFactory.getLogger(TracerClient.class);
  
  protected final Connector connector;
  
  public TracerClient(String instanceName, String zookeepers, String username, String password) throws AccumuloException, AccumuloSecurityException {
    checkNotNull(zookeepers);
    checkNotNull(instanceName);
    checkNotNull(username);
    checkNotNull(password);
    
    ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zookeepers);
    this.connector = instance.getConnector(username, new PasswordToken(password));
  }
  
  public TracerClient(Instance inst, String username, String password) throws AccumuloException, AccumuloSecurityException {
    checkNotNull(inst);
    checkNotNull(username);
    checkNotNull(password);
    
    this.connector = inst.getConnector(username, new PasswordToken(password));
  }
  
  public TracerClient(Connector c) {
    checkNotNull(c);
    
    this.connector = c;
  }
  
  public Tracer getTimings(String uuid) throws NoSuchElementException {
    checkNotNull(uuid);
    
    Scanner scanner;
    try {
      scanner = connector.createScanner(AccumuloTraceStore.TABLE_NAME, new Authorizations());
    } catch (TableNotFoundException e) {
      log.error("Couldn't find the traces table {}", AccumuloTraceStore.TABLE_NAME);
      throw new NoSuchElementException("No trace found for " + uuid);
    }
    scanner.fetchColumnFamily(Tracer.UUID_TEXT);
    scanner.setRange(Range.exact(uuid));
    
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    if (iter.hasNext()) {
      return DeserializeTracer.deserialize(iter.next());
    }
    
    throw new NoSuchElementException("No trace found for " + uuid);
  }
  
  public List<Tracer> mostRecentTimings(int numberOfTimings) {
    Scanner scanner;
    
    try {
      scanner = connector.createScanner(AccumuloTraceStore.TABLE_NAME, new Authorizations());
    } catch (TableNotFoundException e) {
      log.error("Couldn't find the traces table {}", AccumuloTraceStore.TABLE_NAME);
      throw new RuntimeException(e);
    }
    
    scanner.fetchColumnFamily(Tracer.TIME_TEXT);
    scanner.setRange(new Range());
    
    List<Tracer> tracers = Lists.newArrayListWithCapacity(numberOfTimings);
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    for (int i = 0; iter.hasNext() && i < numberOfTimings; i++) {
      Entry<Key,Value> entry = iter.next();
      tracers.add(DeserializeTracer.deserialize(entry));
    }
    
    return tracers;
  }
  
  public Iterable<Tracer> since(Date end) {
    checkNotNull(end);
    
    return since(end.getTime());
  }
  
  public Iterable<Tracer> since(long timeInMillis) {
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
    
    scanner.fetchColumnFamily(Tracer.TIME_TEXT);
    scanner.setRange(new Range(null, false, encodedSince, true));
    
    return Iterables.transform(scanner, new DeserializeTracer());
  }
  
  public Iterable<Tracer> between(Date start, Date end) {
    checkNotNull(start);
    checkNotNull(end);
    
    return between(start.getTime(), end.getTime());
  }
  
  public Iterable<Tracer> between(Long start, Long end) {
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
    
    scanner.fetchColumnFamily(Tracer.TIME_TEXT);
    
    // Since the start and end are also reverse encoded, we use them as normal
    scanner.setRange(new Range(encodedEnd, false, encodedStart, false));
    
    return Iterables.transform(scanner, new DeserializeTracer());
  }
}
