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
package cosmos.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import cosmos.options.Index;
import cosmos.options.Order;
import cosmos.results.Column;

/**
 * Abstracts away the differences between a concrete Set of Index
 * and the IdentitySet.
 */
public class IndexHelper {
  
  protected final Set<Index> indices;
  protected final boolean indexEverything;
  protected HashMultimap<Column,Index> columnsToIndex; 
  
  public IndexHelper(Set<Index> indices) {
    checkNotNull(indices);
    
    this.indices = indices;
    
    if (IdentitySet.class.isAssignableFrom(this.indices.getClass())) {
      indexEverything = true;
    } else {
      indexEverything = false;
      this.columnsToIndex = mapForIndexedColumns(this.indices);
    }
  }
  
  public static IndexHelper create(Set<Index> indices) {
    return new IndexHelper(indices);
  }
  
  public boolean shouldIndex(Column c) {
    if (indexEverything) {
      return true;
    }
    
    return this.columnsToIndex.containsKey(c);
  }
  
  public Set<Index> indices() {
    return Collections.unmodifiableSet(this.indices);
  }
  
  public Set<Index> indicesForColumn(Column c) {
    if (indexEverything) {
      return Sets.newHashSet(Index.define(c, Order.ASCENDING), Index.define(c, Order.DESCENDING));
    }
    
    Set<Index> ret = this.columnsToIndex.get(c);
    
    if (null == ret) {
      // Let the user know that they did something dumb
      // If they checked shouldIndex, they would know that they don't need to call this
      throw new NoSuchElementException("No such index is defined for this column");
    }
    
    return ret;
  }
  
  public Multimap<Column,Index> columnIndices() {
    return ImmutableMultimap.copyOf(this.columnsToIndex);
  }
  
  public int columnCount() {
    if (indexEverything) {
      return Integer.MAX_VALUE;
    }
    
    return columnsToIndex.keySet().size();
  }
  
  protected HashMultimap<Column,Index> mapForIndexedColumns(Iterable<Index> columnsToIndex) {
    final HashMultimap<Column,Index> columns = HashMultimap.create();
    
    for (Index index : columnsToIndex) {
      columns.put(index.column(), index);
    }
    
    return columns;
  }
}
