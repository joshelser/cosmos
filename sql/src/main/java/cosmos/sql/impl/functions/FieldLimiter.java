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
 *  Copyright 2013 
 *
 */
package cosmos.sql.impl.functions;

import java.util.List;
import java.util.Map.Entry;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;

import cosmos.records.impl.MultimapRecord;
import cosmos.records.values.RecordValue;
import cosmos.results.Column;
import cosmos.sql.call.Field;

/**
 * Function, which limits fields from our returned documents.
 * 
 * 
 */

public class FieldLimiter implements Function<MultimapRecord,MultimapRecord> {
  
  Predicate<Entry<Column,RecordValue<?>>> limitingPredicate;
  
  public FieldLimiter(List<Field> fields) {
    limitingPredicate = new FieldLimitPredicate(Lists.newArrayList(Iterables.transform(fields, new Function<Field,String>() {
      @Override
      public String apply(Field field) {
        return field.toString();
      }
    })));
    
  }
  
  @Override
  public MultimapRecord apply(MultimapRecord input) {
    return new FieldLimitingQueryResult(input, input.docId(), limitingPredicate);
  }
  
  private class FieldLimitingQueryResult extends MultimapRecord {
    public FieldLimitingQueryResult(MultimapRecord other, String newDocId, Predicate<Entry<Column,RecordValue<?>>> limitingPredicate) {
      super(other, newDocId);
      document = Multimaps.filterEntries(document, limitingPredicate);
      
    }
  }
  
  /**
   * Internal predicate that limits portions of a document according to the constructed list of fields
   * 
   * @author phrocker
   * 
   */
  private class FieldLimitPredicate implements Predicate<Entry<Column,RecordValue<?>>> {
    
    private List<String> fields;
    
    public FieldLimitPredicate(List<String> fields) {
      this.fields = fields;
    }
    
    @Override
    public boolean apply(Entry<Column,RecordValue<?>> entry) {
      return fields.contains(entry.getKey().name());
    }
  }
  
}
