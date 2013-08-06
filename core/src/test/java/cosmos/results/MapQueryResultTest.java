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

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import cosmos.results.Column;
import cosmos.results.SValue;
import cosmos.results.impl.MapQueryResult;

@RunWith(JUnit4.class)
public class MapQueryResultTest {
  
  @Test
  public void basicCreation() {
    Set<Entry<Column,SValue>> expected = Sets.newHashSet();
    expected.add(Maps.immutableEntry(Column.create("TEXT"),
        SValue.create("foo", new ColumnVisibility("test"))));
    expected.add(Maps.immutableEntry(Column.create("TEXT"),
        SValue.create("bar", new ColumnVisibility("test"))));
    
    Map<String,String> document = Maps.newHashMap();
    document.put("TEXT", "foo");
    document.put("TEXT", "bar");
    
    MapQueryResult mqr = new MapQueryResult(document, "1", new ColumnVisibility("test"),
        new Function<Entry<String,String>,Entry<Column,SValue>>() {

          public Entry<Column,SValue> apply(Entry<String,String> input) {
            final ColumnVisibility cv = new ColumnVisibility("test");
            return Maps.immutableEntry(Column.create(input.getKey()),
                SValue.create(input.getValue(), cv));
          }
    });
    
    for (Entry<Column,SValue> column : mqr.columnValues()) {
      Assert.assertTrue(expected.contains(column));
    }
  }
}
