package sorts;
import java.util.Map.Entry;

import sorts.options.Index;
import sorts.options.Ordering;
import sorts.options.Paging;
import sorts.results.Column;
import sorts.results.PagedQueryResult;
import sorts.results.QueryResult;
import sorts.results.Value;

public interface Sorting {

  /**
   * Add results to the given SortableResult
   * @param id
   * @param queryResults
   */
  public void addResults(SortableResult id, Iterable<QueryResult> queryResults);
  
  /**
   * Add results to the given SortableResult, creating indexes for the provided columns
   * at insertion time.
   * 
   * @param id
   * @param queryResults
   * @param columnsToIndex
   */
  public void addResults(SortableResult id, Iterable<QueryResult> queryResults, 
      Iterable<Index> columnsToIndex);
  
  /**
   * Create indexes for the provided columns for all records that currently exist in
   * the SortableResult
   * @param id
   * @param columnsToIndex
   */
  public void index(SortableResult id, Iterable<Column> columns);
  
  /**
   * Fetch all columns present for a given {@link SortableResult}
   * @param id
   * @return
   */
  public Iterable<Column> columns(SortableResult id);
  
  /**
   * Fetch all results from the given {@link SortableResult}
   * @param id
   * @return
   */
  public Iterable<QueryResult> fetch(SortableResult id);
  
  /**
   * Fetch all results from the given {@link SortableResult}, paging through results 
   * @param id
   * @param limits
   * @return
   */
  public PagedQueryResult fetch(SortableResult id, Paging limits);
  
  /**
   * Fetch results with value for the given {@link Column}
   * @param id
   * @param column
   * @param order
   * @return
   */
  public Iterable<QueryResult> fetch(SortableResult id, Column column);
  
  /**
   * Fetch results with values for the given {@link Column}, paging through results
   * @param id
   * @param column
   * @param order
   * @return
   */
  public Iterable<QueryResult> fetch(SortableResult id, Column column, Paging limits);
  
  /**
   * Fetch results for the given column in the provided {@link Ordering}
   * @param id
   * @param column
   * @param order
   * @return
   */
  public Iterable<QueryResult> fetch(SortableResult id, Ordering ordering);
  
  /**
   * Fetch results for the given column in the provided {@link Ordering}, paging through results
   * @param id
   * @param column
   * @param order
   * @return
   */
  public Iterable<QueryResult> fetch(SortableResult id, Ordering ordering, Paging limits);
  
  /**
   * Return counts for unique values in the given column
   * @param id
   * @param column
   * @param order
   * @return
   */
  public Iterable<Entry<Value,Long>> groupResults(SortableResult id, Column column);
  
  /**
   * Return counts for unique values in the given column, paging through results
   * @param id
   * @param column
   * @param order
   * @return
   */
  public Iterable<Entry<Value,Long>> groupResults(SortableResult id, Column column, Paging limits);
  
  /**
   * Return counts for unique values in the given column
   * @param id
   * @param column
   * @param order
   * @return
   */
  public Iterable<Entry<Value,Long>> groupResults(SortableResult id, Ordering order);
  
  /**
   * Return counts for unique values in the given column, paging through results
   * @param id
   * @param column
   * @param order
   * @return
   */
  public Iterable<Entry<Value,Long>> groupResults(SortableResult id, Ordering order, Paging limits);
  
  /**
   * Clean up references to the data referenced by this SortableResult
   * @param id
   */
  public void delete(SortableResult id);
}
