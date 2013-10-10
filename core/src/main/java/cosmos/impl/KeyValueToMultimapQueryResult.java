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

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.DataInputBuffer;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import cosmos.records.impl.MultimapRecord;

/**
 * 
 */
public class KeyValueToMultimapQueryResult implements Function<Entry<Key,Value>,MultimapRecord> {

  private static final KeyValueToMultimapQueryResult INSTANCE = new KeyValueToMultimapQueryResult();
  
  public MultimapRecord apply(Entry<Key,Value> input) {
    DataInputBuffer buf = new DataInputBuffer();
    buf.reset(input.getValue().get(), input.getValue().getSize());
    
    try {
      return MultimapRecord.recreate(buf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static MultimapRecord transform(Entry<Key,Value> input) {
    return INSTANCE.apply(input);
  }
  
  public static MultimapRecord transform(Value input) {
    return INSTANCE.apply(Maps.immutableEntry((Key) null, input));
  }
  
}
