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
package cosmos.results;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

public class Column implements Writable {
  private String column;
  
  protected Column() { }
  
  public Column(String column) {
    Preconditions.checkNotNull(column);
    this.column = column;
  }

  public String column() {
    return this.column;
  }
  
  public static Column create(String column) {
    Preconditions.checkNotNull(column);
    
    return new Column(column);
  }
  
  public static Column recreate(DataInput in) throws IOException {
    final Column column = new Column();
    column.readFields(in);
    return column;
  }
  
  @Override
  public int hashCode() {
    return this.column.hashCode();
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof Column) {
      return this.column.equals(((Column) o).column());
    }
    
    return false;
  }
  
  @Override
  public String toString() {
    return column.toString();
  }

  public void write(DataOutput out) throws IOException {
    Text.writeString(out, this.column);
  }

  public void readFields(DataInput in) throws IOException {
    this.column = Text.readString(in);
  }
}
