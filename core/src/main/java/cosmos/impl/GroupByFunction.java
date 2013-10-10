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
package cosmos.impl;

import java.nio.charset.CharacterCodingException;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import cosmos.accumulo.GroupByRowSuffixIterator;
import cosmos.options.Defaults;
import cosmos.records.RecordValue;

/**
 * 
 */
public class GroupByFunction implements Function<Entry<Key,Value>,Entry<RecordValue,Long>> {

  private final Text _holder = new Text();
  
  @Override
  public Entry<RecordValue,Long> apply(Entry<Key,Value> entry) {
    String value = getValueFromKey(entry.getKey());
    
    //TODO Add Cache for CV
    RecordValue sval = RecordValue.create(value, entry.getKey().getColumnVisibilityParsed());
    VLongWritable writable = GroupByRowSuffixIterator.getWritable(entry.getValue());
    
    return Maps.immutableEntry(sval, writable.get());
  }
  
  private String getValueFromKey(Key k) {
    Preconditions.checkNotNull(k);
    
    k.getRow(_holder);
    
    int index = _holder.find(Defaults.NULL_BYTE_STR);
    
    if (-1 == index) {
      throw new IllegalArgumentException("Found no null byte in key: " + k);
    }
    
    try {
      return Text.decode(_holder.getBytes(), index + 1, _holder.getLength() - (index + 1));
    } catch (CharacterCodingException e) {
      throw new IllegalArgumentException(e);
    }
  }
  
}
