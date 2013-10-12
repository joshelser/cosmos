/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cosmos.records;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.gson.JsonParseException;

import cosmos.options.Defaults;
import cosmos.records.impl.MapRecord;
import cosmos.records.impl.MultimapRecord;
import cosmos.records.values.IntegerRecordValue;
import cosmos.records.values.LongRecordValue;
import cosmos.records.values.RecordValue;
import cosmos.records.values.StringRecordValue;
import cosmos.results.Column;

/**
 * 
 */
public class JsonRecordsTest {

  @Test
  public void simpleJsonTest() {
    String json = "[{'foo1':'bar1', 'foo2':'bar2'}, {'foo3':'bar3', 'foo4':'bar4'}]";

    List<MapRecord> records = JsonRecords.parseAsMap(json);

    Assert.assertEquals(2, records.size());

    Assert.assertEquals(
        new MapRecord(ImmutableMap.<Column,RecordValue<?>> of(Column.create("foo1"), RecordValue.create("bar1"), Column.create("foo2"),
            RecordValue.create("bar2")), "0", Defaults.EMPTY_VIS), records.get(0));

    Assert.assertEquals(
        new MapRecord(ImmutableMap.<Column,RecordValue<?>> of(Column.create("foo3"), RecordValue.create("bar3"), Column.create("foo4"),
            RecordValue.create("bar4")), "1", Defaults.EMPTY_VIS), records.get(1));
  }

  @Test(expected = JsonParseException.class)
  public void invalidJsonMap() {
    String json = "[{'foo1";

    JsonRecords.parseAsMap(json);
  }

  @Test(expected = JsonParseException.class)
  public void invalidJsonMultimap() {
    String json = "[{'foo1";

    JsonRecords.parseAsMultimap(json);
  }

  @Test(expected = JsonParseException.class)
  public void nonSupportedJsonMap() {
    String json = "{'foo1':'bar1'}";

    JsonRecords.parseAsMap(json);
  }

  @Test(expected = JsonParseException.class)
  public void nonSupportedJsonMultimap() {
    String json = "{'foo1':'bar1'}";

    JsonRecords.parseAsMultimap(json);
  }

  @Test
  public void emptyJson() {
    String json = "[]";

    Assert.assertEquals(0, JsonRecords.parseAsMap(json).size());
    Assert.assertEquals(0, JsonRecords.parseAsMultimap(json).size());

    json = "";

    Assert.assertEquals(0, JsonRecords.parseAsMap(json).size());
    Assert.assertEquals(0, JsonRecords.parseAsMultimap(json).size());
  }

  @Test
  public void mixedNumericJson() {
    String json = "[ {'foo1':'bar1', 'foo2':2} ]";

    List<MapRecord> records = JsonRecords.parseAsMap(json);
    Assert.assertEquals(1, records.size());
    
    MapRecord record = records.get(0);
    
    Column c = Column.create("foo1");
    Assert.assertTrue("Map does not contain key: " + record, record.containsKey(c));
    Assert.assertEquals(StringRecordValue.class, record.get(c).getClass());
    Assert.assertEquals("bar1", record.get(c).value());

    c = Column.create("foo2");
    Assert.assertTrue("Map does not contain key: " + record, record.containsKey(c));
    Assert.assertEquals(IntegerRecordValue.class, record.get(c).getClass());
    Assert.assertEquals(2, record.get(c).value());
  }

  @Test
  public void longJson() {
    String json = "[ {'foo2':" + Long.MAX_VALUE + "} ]";

    List<MapRecord> records = JsonRecords.parseAsMap(json);
    Assert.assertEquals(1, records.size());
    
    MapRecord record = records.get(0);
    
    Column c = Column.create("foo2");
    Assert.assertTrue("Map does not contain key: " + record, record.containsKey(c));
    Assert.assertEquals(LongRecordValue.class, record.get(c).getClass());
    Assert.assertEquals(Long.MAX_VALUE, record.get(c).value());
  }
  
  @Test
  public void duplicateKeysInMap() {
    String json = "[ {'foo':1, 'foo':2} ]";
    
    MapRecord record = JsonRecords.parseAsMap(json).get(0);
    
    Assert.assertEquals(2, record.get(Column.create("foo")).value());
  }
  
  @Test
  public void duplicateKeysInMultimap() {
    String json = "[ {'foo':1, 'foo':2} ]";
    
    MultimapRecord record = JsonRecords.parseAsMultimap(json).get(0);
    
    Collection<RecordValue<?>> values = record.get(Column.create("foo"));
    Assert.assertEquals(1, values.size());
    Assert.assertEquals(2, values.iterator().next().value());
  }
  
  @Test
  public void arrayValueInMultimap() {
    String json = "[ {'foo':[1, 2]} ]";
    
    MultimapRecord record = JsonRecords.parseAsMultimap(json).get(0);
    
    Collection<RecordValue<?>> values = record.get(Column.create("foo"));
    Set<Integer> ints = Sets.newHashSet();
    for (RecordValue<?> value : values) {
      ints.add(((IntegerRecordValue) value).value());
    }
    
    Assert.assertEquals(ImmutableSet.of(1, 2), ints);
  }
  
  @Test(expected = RuntimeException.class)
  public void nestedArrayInValue() {
    String json = "[ {'foo':[1, [2]]} ]";
    
    JsonRecords.parseAsMultimap(json);
  }
  
  @Test(expected = RuntimeException.class)
  public void objectInArrayInValue() {
    String json = "[ {'foo':[1, {'foo':2} ]} ]";
    
    JsonRecords.parseAsMultimap(json);
  }
}
