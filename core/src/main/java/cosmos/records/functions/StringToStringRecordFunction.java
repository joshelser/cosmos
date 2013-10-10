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
package cosmos.records.functions;

import java.util.Map.Entry;

import org.apache.accumulo.core.security.ColumnVisibility;

import com.google.common.collect.Maps;

import cosmos.options.Defaults;
import cosmos.records.RecordFunction;
import cosmos.records.RecordValue;
import cosmos.results.Column;

/**
 * Converts an {@link Entry<String,String>} to {@link Entry<Column,RecordValue>} using an empty {@link ColumnVisibility} 
 */
public class StringToStringRecordFunction implements RecordFunction<String,String> {

  @Override
  public Entry<Column,RecordValue> apply(Entry<String,String> input) {
    // Slightly duplicative, but making an inner instance of StringToStringWithVisibility is just as bad
    // in terms of duplicate code and the inheritance of that constructor would just be weird.
    return Maps.immutableEntry(Column.create(input.getKey()), RecordValue.create(input.getValue(), Defaults.EMPTY_VIS));
  }

}