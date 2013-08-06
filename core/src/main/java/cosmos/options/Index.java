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

import cosmos.results.Column;

public class Index {
  
  protected final Column column;
  protected final Order order;
  
  public Index(Column column) {
    this(column, Order.ASCENDING);
  }
  
  public Index(Column column, Order order) {
    Preconditions.checkNotNull(column);
    Preconditions.checkNotNull(order);
    
    this.column = column;
    this.order = order;
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
  
  public static Index define(Column column, Order order) {
    return new Index(column, order);
  }

  public Column column() {
    return this.column;
  }

  public Order order() {
    return this.order;
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof Index) {
      Index other = (Index) o;
      
      if (this.column.equals(other.column) && this.order.equals(other.order)) {
        return true;
      }
    }
    
    return false;
  }
  
  @Override
  public int hashCode() {
    return this.column.hashCode() ^ this.order.hashCode();
  }
  
  @Override
  public String toString() {
    return this.column + ", " + this.order;
  }
  
}
