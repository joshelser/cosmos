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

package cosmos.statistics;

import cosmos.results.Column;
import cosmos.statistics.store.Cardinality;
import cosmos.statistics.store.Count;
import cosmos.store.Store;

public class ColumnStatistics implements IndexSelectivity {

	Count count;

	Cardinality card;

	/**
	 * Store references to the column and store.
	 */
	protected Store store;

	protected Column column;

	ColumnStatistics(Store store, Column column) {

		this.store = store;
		
		this.column = column;
		
		count = new Count(Long.MAX_VALUE);

		card = new Cardinality(Long.MAX_VALUE);

	}

	@Override
	public boolean isDelayed() {
		return true;
	}

	@Override
	public double selectivity() {
		// remember your rules of math ... / then * ;)
		return Math.ceil(cardinality().get() / countEstimate().get() * 100);
	}

	@Override
	public Cardinality cardinality() {
		return card;
	}

	@Override
	public Count countEstimate() {
		return count;
	}
	
	public Column getColumn()
	{
		return column;
	}

}
