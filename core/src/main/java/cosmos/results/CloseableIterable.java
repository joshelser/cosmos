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
package cosmos.results;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;

import cosmos.trace.Tracer;

/**
 * 
 */
public class CloseableIterable<T> implements Results<T> {
  
  protected final ScannerBase scanner;
  protected final Iterable<T> iterable;
  protected final Tracer tracer;
  protected final String description;
  protected final Stopwatch sw;
  
  public CloseableIterable(ScannerBase scanner, Iterable<T> iterable, Tracer t, String desc, Stopwatch sw) {
    checkNotNull(scanner);
    checkNotNull(iterable);
    checkNotNull(t);
    checkNotNull(desc);
    checkNotNull(sw);
    
    this.scanner = scanner;
    this.iterable = iterable;
    this.tracer = t;
    this.description = desc;
    this.sw = sw;
  }
  
  public static <T> CloseableIterable<T> create(ScannerBase scanner, Iterable<T> iterable, Tracer t, String desc, Stopwatch sw) {
    return new CloseableIterable<T>(scanner, iterable, t, desc, sw);
  }
  
  public static <T> CloseableIterable<T> transform(ScannerBase scanner, Function<Entry<Key,Value>,T> func, Tracer t, String desc, Stopwatch sw) {
    return new CloseableIterable<T>(scanner, Iterables.transform(scanner, func), t, desc, sw);
  }
  
  public static <T> CloseableIterable<T> filterAndTransform(ScannerBase scanner, Predicate<Entry<Key,Value>> filter, Function<Entry<Key,Value>,T> func,
      Tracer t, String desc, Stopwatch sw) {
    return new CloseableIterable<T>(scanner, Iterables.transform(Iterables.filter(scanner, filter), func), t, desc, sw);
  }
  
  protected ScannerBase source() {
    return scanner;
  }
  
  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      final Iterator<T> delegate = iterable.iterator();
      
      @Override
      public boolean hasNext() {
        boolean hasNext = delegate.hasNext();
        
        if (!hasNext && sw.isRunning()) {
          sw.stop();
          tracer.addTiming(description, sw.elapsed(TimeUnit.MILLISECONDS));
        }
        
        return hasNext;
      }
      
      @Override
      public T next() {
        return delegate.next();
      }
      
      @Override
      public void remove() {
        delegate.remove();
      }
      
    };
  }
  
  @Override
  public void close() {
    // Client may close the Iterable before exhausting the records
    if (sw.isRunning()) {
      sw.stop();
      tracer.addTiming(description, sw.elapsed(TimeUnit.MILLISECONDS));
    }
    
    if (!(this.scanner instanceof Scanner)) {
      ((BatchScanner) scanner).close();
    }
  }
}
