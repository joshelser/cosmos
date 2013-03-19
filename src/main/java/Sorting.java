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
   * 
   * @param id
   * @return
   */
  public Iterable<QueryResult> fetch(SortableResult id);
  
  public PagedQueryResult fetchWithPaging(SortableResult id, Paging limits);
  
  public Iterable<QueryResult> fetchWithOrdering(SortableResult id, String column, Order order);
  
  public Iterable<Entry<Value,Long>> groupResults(SortableResult id, String column, Order order);
  
  // Delete results
  public void delete(SortableResult id);
}
