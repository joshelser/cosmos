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
package cosmos;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;

import com.google.common.collect.Ordering;

import cosmos.options.Index;
import cosmos.options.Paging;
import cosmos.results.CloseableIterable;
import cosmos.results.Column;
import cosmos.results.PagedQueryResult;
import cosmos.results.QueryResult;
import cosmos.results.SValue;
import cosmos.results.impl.MultimapQueryResult;
import cosmos.store.Store;

public interface Cosmos {
  
	/**
	 * 
	 * @param id
	 * @throws TableNotFoundException
	 * @throws MutationsRejectedException
	 * @throws UnexpectedStateException
	 */
  public void register(Store id) throws TableNotFoundException, MutationsRejectedException, UnexpectedStateException;
  
  /**
   * Adds a result to the given SortableResult
   * 
   * @param id
   * @param queryResult
   * @throws Exception
   */
  public void addResult(Store id, QueryResult<?> queryResult) throws Exception;
  
  /**
   * Add results to the given SortableResult
   * 
   * @param id
   * @param queryResults
   */
  public void addResults(Store id, Iterable<? extends QueryResult<?>> queryResults) throws Exception;
  
  /**
   * Closes the state of the given SortableResult. No additional results can be written after the set has been finalized. 
   * @param id
   * @throws TableNotFoundException
   * @throws MutationsRejectedException
   * @throws UnexpectedStateException
   */
  public void finalize(Store id) throws TableNotFoundException, MutationsRejectedException, UnexpectedStateException;
  
  /**
   * Create indexes for the provided columns for all records that currently exist in the SortableResult
   * 
   * @param id
   * @param columnsToIndex
   */
  public void index(Store id, Set<Index> columnsToIndex) throws Exception;
  
  /**
   * Fetch all columns present for a given {@link Store}
   * 
   * @param id
   * @return
   */
  public CloseableIterable<Column> columns(Store id) throws TableNotFoundException, UnexpectedStateException;
  
  /**
   * Fetch all results from the given {@link Store}
   * 
   * @param id
   * @return
   */
  public CloseableIterable<MultimapQueryResult> fetch(Store id) throws TableNotFoundException, UnexpectedStateException;
  
  /**
   * Fetch all results from the given {@link Store}, paging through results
   * 
   * @param id
   * @param limits
   * @return
   */
  public PagedQueryResult<MultimapQueryResult> fetch(Store id, Paging limits) throws TableNotFoundException, UnexpectedStateException;
  
  /**
   * Fetch results with the given {@link value} in the given {@link Column}
   * 
   * @param id
   * @param column
   * @param order
   * @return
   */
  public CloseableIterable<MultimapQueryResult> fetch(Store id, Column column, String value) throws TableNotFoundException, UnexpectedStateException, UnindexedColumnException;
  
  /**
   * Fetch results with values for the given {@link Column}, paging through results
   * 
   * @param id
   * @param column
   * @param order
   * @return
   */
  public PagedQueryResult<MultimapQueryResult> fetch(Store id, Column column, String value, Paging limits) throws TableNotFoundException, UnexpectedStateException, UnindexedColumnException;
  
  /**
   * Fetch results in the provided {@link Ordering}
   * 
   * @param id
   * @param column
   * @param order
   * @return
   */
  public CloseableIterable<MultimapQueryResult> fetch(Store id, Index ordering) throws TableNotFoundException, UnexpectedStateException, UnindexedColumnException;

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
  public CloseableIterable<MultimapQueryResult> fetch(Store id, Index ordering, boolean duplicateUidsAllowed) throws TableNotFoundException, UnexpectedStateException, UnindexedColumnException;

  /**
   * Fetch results in the provided {@link Ordering}, paging through results
   * 
   * @param id
   * @param column
   * @param order
   * @return
   */
  public PagedQueryResult<MultimapQueryResult> fetch(Store id, Index ordering, Paging limits) throws TableNotFoundException, UnexpectedStateException, UnindexedColumnException;
  
  /**
   * Return counts for unique values in the given column
   * 
   * @param id
   * @param column
   * @param order
   * @return
   */
  public CloseableIterable<Entry<SValue,Long>> groupResults(Store id, Column column) throws TableNotFoundException, UnexpectedStateException, UnindexedColumnException;
  
  /**
   * Return counts for unique values in the given column, paging through results
   * 
   * @param id
   * @param column
   * @param order
   * @return
   */
  public PagedQueryResult<Entry<SValue,Long>> groupResults(Store id, Column column, Paging limits) throws TableNotFoundException, UnexpectedStateException, UnindexedColumnException;
  
  /**
   * Given a docId contained in the {@link Store}, fetch the record  
   * @param id
   * @param docId
   * @return
   * @throws TableNotFoundException
   * @throws UnexpectedStateException
   */
  public MultimapQueryResult contents(Store id, String docId) throws TableNotFoundException, UnexpectedStateException;
  
  /**
   * Clean up references to the data referenced by this SortableResult
   * 
   * @param id
   */
  public void delete(Store id) throws TableNotFoundException, MutationsRejectedException, UnexpectedStateException;

  
  /**
   * Cleans up internal resources, such as the Curator/ZooKeeper connection, and should be called by the client
   */
  public void close();
}
