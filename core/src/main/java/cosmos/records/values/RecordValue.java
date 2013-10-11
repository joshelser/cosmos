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
package cosmos.records.values;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Preconditions;

import cosmos.options.Defaults;

public abstract class RecordValue<T extends Comparable<T>> implements Writable, Comparable<RecordValue<T>> {
  protected T value;
  protected ColumnVisibility visibility;
  
  protected RecordValue() { }
  
  protected RecordValue(T value, ColumnVisibility visibility) {
    Preconditions.checkNotNull(value);
    Preconditions.checkNotNull(visibility);
    this.value = value;
    this.visibility = visibility;
  }
  
  public T value() {
    return this.value;
  }
  
  public ColumnVisibility visibility() {
    return this.visibility;
  }
  
  public static RecordValue<?> recreate(DataInput in) throws IOException {
    String clzName = WritableUtils.readString(in);
    
    Class<?> clz;
    try {
      clz = Class.forName(clzName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    
    if (!RecordValue.class.isAssignableFrom(clz)) {
      throw new IOException("Cannot deserialize class: " + clzName);
    }
    
    RecordValue<?> rec = null;
    try {
      rec = (RecordValue<?>) clz.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    
    rec.readFields(in);
    
    return rec;
  }
  
  @Override
  public int hashCode() {
    return this.value.hashCode() ^ this.visibility.hashCode();
  }
  
  @Override
  public boolean equals(Object o) {
    if (RecordValue.class.isAssignableFrom(o.getClass())) {
      RecordValue<?> other = (RecordValue<?>) o;
      return this.value.equals(other.value) &&
          this.visibility.equals(other.visibility);
    }
    
    return false;
  }
  
  @Override
  public String toString() {
    return "\"" + value + "\" " + visibility;
  }

  protected void writeVisibility(DataOutput out) throws IOException {
    WritableUtils.writeString(out, this.getClass().getName());
    
    byte[] cvBytes = this.visibility.getExpression();
    WritableUtils.writeVInt(out, cvBytes.length);
    out.write(cvBytes);
  }
  
  public void readVisibility(DataInput in) throws IOException {
    
    final int cvLength = WritableUtils.readVInt(in);
    final byte[] cvBytes = new byte[cvLength];
    in.readFully(cvBytes);

    this.visibility = new ColumnVisibility(cvBytes);
  }
  
  protected int visibilityCompare(RecordValue<T> o) {
    return this.visibility.toString().compareTo(o.visibility.toString());
  }

  @Override
  public int compareTo(RecordValue<T> o) {
    int res = this.value.compareTo(o.value);
    
    if (0 == res) {
      return this.visibility.toString().compareTo(o.visibility.toString());
    }
    
    return res;
  }
  
  public static RecordValue<?> create(Object o) {
    return create(o, Defaults.EMPTY_VIS);
  }
  
  // TODO There has to be a better paradigm to codify this...
  public static RecordValue<?> create(Object o, ColumnVisibility cv) {
    Preconditions.checkNotNull(o);
    Preconditions.checkNotNull(cv);
    
    if (o instanceof String) {
      return new StringRecordValue((String) o, cv);
    } else if (o instanceof Integer) {
      return new IntegerRecordValue((Integer) o, cv);
    } else if (o instanceof Long) {
      return new LongRecordValue((Long) o, cv);
    } else {
      throw new UnsupportedOperationException("Can't handle value of type " + o.getClass());
    }
  }
}
