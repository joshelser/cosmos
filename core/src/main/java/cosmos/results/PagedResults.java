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
import java.util.List;

import com.google.common.collect.Iterables;

import cosmos.options.Paging;

public class PagedResults<T> implements Results<List<T>> {

  protected final CloseableIterable<T> source;
  protected final Iterable<List<T>> pagedLimitedResults;
  
  public static <T> PagedResults<T> create(CloseableIterable<T> results, Paging limits) {
    return new PagedResults<T>(results, limits);
  }
  
  public PagedResults(CloseableIterable<T> results, Paging limits) {
    checkNotNull(results);
    checkNotNull(limits);
    
    source = results;
    pagedLimitedResults = Iterables.partition(Iterables.limit(results, limits.maxResults()), limits.pageSize());
  }
  
  @Override
  public Iterator<List<T>> iterator() {
    return pagedLimitedResults.iterator();
  }
  
  @Override
  public void close() {
    this.source.close();
  }
}
