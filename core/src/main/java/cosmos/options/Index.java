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
package cosmos.options;

import com.google.common.base.Preconditions;
import java.lang.reflect.Type;

import cosmos.results.Column;

public class Index {

	protected final Column column;
	protected final Order order;
	protected final Class<?> indexedType;

	public Index(Column column) {
		this(column, Order.ASCENDING, String.class);
	}

	public Index(Column column, Order order, Class<?> indexedType) {
		Preconditions.checkNotNull(column);
		Preconditions.checkNotNull(order);
		Preconditions.checkNotNull(indexedType);

		this.column = column;
		this.order = order;
		this.indexedType = indexedType;
	}

	public static Index define(String columnName) {
		return define(Column.create(columnName));
	}

	public static Index define(Column column) {
		return new Index(column);
	}

	public static Index define(String columnName, Order order) {
		return define(Column.create(columnName), order);
	}

	public static Index define(Column column, Order order, Class<?> indexedType) {
		return new Index(column, order, indexedType);
	}

	public static Index define(Column column, Order order) {
		return new Index(column, order, String.class);
	}

	public Column column() {
		return this.column;
	}

	public Order order() {
		return this.order;
	}

	/**
	 * Returns the indexed type.
	 * @return 
	 */
	public Class<?> getIndexTyped() {
		return indexedType;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof Index) {
			Index other = (Index) o;

			if (this.column.equals(other.column)
					&& this.order.equals(other.order)) {

				return indexedType.equals(other.indexedType);

			}
		}

		return false;
	}

	@Override
	public int hashCode() {
		return (this.column.hashCode() ^ this.order.hashCode())
				+ (indexedType.hashCode() + 17);
	}

	@Override
	public String toString() {
		return this.column + ", " + this.order;
	}

}
