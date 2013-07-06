package sorts.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Set;

import sorts.options.Index;
import sorts.options.Order;
import sorts.results.Column;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

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
