package sorts;

import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;

import sorts.impl.SortableResult;
import sorts.options.Index;
import sorts.options.Paging;
import sorts.results.CloseableIterable;
import sorts.results.Column;
import sorts.results.PagedQueryResult;
import sorts.results.QueryResult;
import sorts.results.SValue;
import sorts.results.impl.MultimapQueryResult;

import com.google.common.collect.Ordering;

public interface Sorting {
  
  public void register(SortableResult id) throws TableNotFoundException, MutationsRejectedException, UnexpectedStateException;
  
  /**
   * Adds a result to the given SortableResult
   * 
   * @param id
   * @param queryResult
   * @throws Exception
   */
  public void addResult(SortableResult id, QueryResult<?> queryResult) throws Exception;
  
  /**
   * Add results to the given SortableResult
   * 
   * @param id
   * @param queryResults
   */
  public void addResults(SortableResult id, Iterable<QueryResult<?>> queryResults) throws Exception;
  
  /**
   * Closes the state of the given SortableResult. No additional results can be written after the set has been finalized. 
   * @param id
   * @throws TableNotFoundException
   * @throws MutationsRejectedException
   * @throws UnexpectedStateException
   */
  public void finalize(SortableResult id) throws TableNotFoundException, MutationsRejectedException, UnexpectedStateException;
  
  /**
   * Create indexes for the provided columns for all records that currently exist in the SortableResult
   * 
   * @param id
   * @param columnsToIndex
   */
  public void index(SortableResult id, Set<Index> columnsToIndex) throws Exception;
  
  /**
   * Fetch all columns present for a given {@link SortableResult}
   * 
   * @param id
   * @return
   */
  public Iterable<Column> columns(SortableResult id) throws TableNotFoundException, UnexpectedStateException;
  
  /**
   * Fetch all results from the given {@link SortableResult}
   * 
   * @param id
   * @return
   */
  public CloseableIterable<MultimapQueryResult> fetch(SortableResult id) throws TableNotFoundException, UnexpectedStateException;
  
  /**
   * Fetch all results from the given {@link SortableResult}, paging through results
   * 
   * @param id
   * @param limits
   * @return
   */
  public PagedQueryResult<MultimapQueryResult> fetch(SortableResult id, Paging limits) throws TableNotFoundException, UnexpectedStateException;
  
  /**
   * Fetch results with the given {@link value} in the given {@link Column}
   * 
   * @param id
   * @param column
   * @param order
   * @return
   */
  public CloseableIterable<MultimapQueryResult> fetch(SortableResult id, Column column, String value) throws TableNotFoundException, UnexpectedStateException, UnindexedColumnException;
  
  /**
   * Fetch results with values for the given {@link Column}, paging through results
   * 
   * @param id
   * @param column
   * @param order
   * @return
   */
  public PagedQueryResult<MultimapQueryResult> fetch(SortableResult id, Column column, String value, Paging limits) throws TableNotFoundException, UnexpectedStateException, UnindexedColumnException;
  
  /**
   * Fetch results in the provided {@link Ordering}
   * 
   * @param id
   * @param column
   * @param order
   * @return
   */
  public CloseableIterable<MultimapQueryResult> fetch(SortableResult id, Index ordering) throws TableNotFoundException, UnexpectedStateException, UnindexedColumnException;

  /**
   * Fetch results in the provided {@link Ordering}. If {@link duplicateUidsAllowed} is true,
   * records with multiple values for the {@link Column} specified by the {@link ordering} will
   * only be returned once.
   * 
   * @param id
   * @param ordering
   * @param duplicateUidsAllowed
   * @return
   * @throws TableNotFoundException
   * @throws UnexpectedStateException
   * @throws UnindexedColumnException
   */
  public CloseableIterable<MultimapQueryResult> fetch(SortableResult id, Index ordering, boolean duplicateUidsAllowed) throws TableNotFoundException, UnexpectedStateException, UnindexedColumnException;

  /**
   * Fetch results in the provided {@link Ordering}, paging through results
   * 
   * @param id
   * @param column
   * @param order
   * @return
   */
  public PagedQueryResult<MultimapQueryResult> fetch(SortableResult id, Index ordering, Paging limits) throws TableNotFoundException, UnexpectedStateException, UnindexedColumnException;
  
  /**
   * Return counts for unique values in the given column
   * 
   * @param id
   * @param column
   * @param order
   * @return
   */
  public CloseableIterable<Entry<SValue,Long>> groupResults(SortableResult id, Column column) throws TableNotFoundException, UnexpectedStateException, UnindexedColumnException;
  
  /**
   * Return counts for unique values in the given column, paging through results
   * 
   * @param id
   * @param column
   * @param order
   * @return
   */
  public PagedQueryResult<Entry<SValue,Long>> groupResults(SortableResult id, Column column, Paging limits) throws TableNotFoundException, UnexpectedStateException, UnindexedColumnException;
  
  /**
   * Clean up references to the data referenced by this SortableResult
   * 
   * @param id
   */
  public void delete(SortableResult id) throws TableNotFoundException, MutationsRejectedException, UnexpectedStateException;
  
  /**
   * Cleans up internal resources, such as the Curator/ZooKeeper connection, and should be called by the client
   */
  public void close();
}
