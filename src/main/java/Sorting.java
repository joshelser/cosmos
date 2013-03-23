import java.util.Map.Entry;

public interface Sorting {
  /**
   * Create an object which must be passed to all operations
   * to store, fetch and delete results from Accumulo. 
   * @return
   */
  public SortableResult register();

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
  public void addResultsWithIndex(SortableResult id, Iterable<QueryResult> queryResults, 
      Iterable<Index> columnsToIndex);
  
  /**
   * Create indexes for the provided columns for all records that currently exist in
   * the SortableResult
   * @param id
   * @param columnsToIndex
   */
  public void index(SortableResult id, Iterable<Index> columnsToIndex);
  
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
   * Fetch results from the given {@link SortableResult}, paging through results 
   * @param id
   * @param limits
   * @return
   */
  public PagedQueryResult fetch(SortableResult id, Paging limits);
  
  /**
   * Fetch results for the given column in the provided {@link Order}
   * @param id
   * @param column
   * @param order
   * @return
   */
  public Iterable<QueryResult> fetch(SortableResult id, Ordering ordering);
  
  /**
   * Return counts for unique values in the given column
   * @param id
   * @param column
   * @param order
   * @return
   */
  public Iterable<Entry<Value,Long>> groupResults(SortableResult id, String column, Order order);
  
  /**
   * Clean up references to the data referenced by this SortableResult
   * @param id
   */
  public void delete(SortableResult id);
}
