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

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import cosmos.records.RecordFunction;
import cosmos.records.impl.MapRecord;
import cosmos.records.values.RecordValue;

@RunWith(JUnit4.class)
public class MapQueryResultTest {
  
  @Test
  public void basicCreation() {
    Set<Entry<Column,RecordValue<?>>> expected = Sets.newHashSet();
    expected.add(Maps.<Column,RecordValue<?>> immutableEntry(Column.create("TEXT"),
        RecordValue.create("foo", new ColumnVisibility("test"))));
    expected.add(Maps.<Column,RecordValue<?>> immutableEntry(Column.create("TEXT"),
        RecordValue.create("bar", new ColumnVisibility("test"))));
    
    Map<String,String> document = Maps.newHashMap();
    document.put("TEXT", "foo");
    document.put("TEXT", "bar");
    
    MapRecord mqr = new MapRecord(document, "1", new ColumnVisibility("test"),
        new RecordFunction<String,String>() {

          public Entry<Column,RecordValue<?>> apply(Entry<String,String> input) {
            final ColumnVisibility cv = new ColumnVisibility("test");
            return Maps.<Column,RecordValue<?>> immutableEntry(Column.create(input.getKey()),
                RecordValue.create(input.getValue(), cv));
          }
    });
    
    for (Entry<Column,RecordValue<?>> column : mqr.columnValues()) {
      Assert.assertTrue(expected.contains(column));
    }
  }
}
