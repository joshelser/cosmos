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
 *  Copyright 2013 
 *
 */
package cosmos.results.streaming;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.TableNotFoundException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import cosmos.options.Index;
import cosmos.results.Column;
import cosmos.results.QueryResult;
import cosmos.results.SValue;
import cosmos.statistics.IndexStatistics;
import cosmos.statistics.SchemaStatistics;
import cosmos.statistics.store.Count;
import cosmos.store.Store;

/**
 * defines the statistics engine
 * 
 */
public class StatsProcessor implements Processor {

	private Store store;

	protected Collection<IndexStatistics> rowStats;

	protected Map<Column, IndexStatistics> statsMap;

	public StatsProcessor(Store store) throws TableNotFoundException {
		this.store = store;

		rowStats = SchemaStatistics.storeAndUpdate(store,
				store.columnsToIndex());

		statsMap = Maps.newHashMap();

		for (IndexStatistics stat : rowStats) {
			Preconditions.checkNotNull(stat);
			statsMap.put(stat.getColumn(), stat);
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * cosmos.results.streaming.Processor#execute(cosmos.results.QueryResult)
	 */
	@Override
	public void execute(QueryResult<?> result) {
		
	
	}
		// no op

	@Override
	public void flush() throws IOException
	{
		for(IndexStatistics  stat : rowStats )
			try {
				SchemaStatistics.storeAndUpdate(store, new Index(stat.getColumn()), stat);
			} catch (TableNotFoundException e) {
				throw new IOException(e);
			}
	}

	@Override
	public void execute(Entry<Column, SValue> entry) {
		// update the statistsics
		IndexStatistics stat = statsMap.get(entry.getKey());

		if (stat != null) {
			stat.cardinality().update(entry.getValue().toString());
			stat.countEstimate().aggregate(new Count(1));
		}

	}

}
