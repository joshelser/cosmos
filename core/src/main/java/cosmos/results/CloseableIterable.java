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

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

/**
 * 
 */
public class CloseableIterable<T> implements Results<T> {
  
  protected final ScannerBase scanner;
  protected final Iterable<T> iterable;
  
  public CloseableIterable(ScannerBase scanner, Iterable<T> iterable) {
    checkNotNull(scanner);
    checkNotNull(iterable);
    
    this.scanner = scanner;
    this.iterable = iterable;
  }
  
  public static <T> CloseableIterable<T> create(ScannerBase scanner, Iterable<T> iterable) {
    return new CloseableIterable<T>(scanner, iterable);
  }
  
  public static <T> CloseableIterable<T> transform(ScannerBase scanner, Function<Entry<Key,Value>,T> func) {
    return new CloseableIterable<T>(scanner, Iterables.transform(scanner, func));
  }
  
  public static <T> CloseableIterable<T> filterAndTransform(ScannerBase scanner, Predicate<Entry<Key,Value>> filter, Function<Entry<Key,Value>,T> func) {
    return new CloseableIterable<T>(scanner, Iterables.transform(Iterables.filter(scanner, filter), func));
  }
  
  protected ScannerBase source() {
    return scanner;
  }
  
  @Override
  public Iterator<T> iterator() {
    return iterable.iterator();
  }
  
  @Override
  public void close() {
    scanner.close();
  }
  
}
