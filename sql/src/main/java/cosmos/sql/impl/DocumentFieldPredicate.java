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
package cosmos.sql.impl;

import java.util.Collection;

import com.google.common.base.Predicate;

import cosmos.results.Column;
import cosmos.results.SValue;
import cosmos.results.impl.MultimapQueryResult;
import cosmos.sql.call.Field;
import cosmos.sql.call.Literal;

public class DocumentFieldPredicate implements Predicate<MultimapQueryResult> {

  private Column column;
  private Literal predicateValue;

  public DocumentFieldPredicate(Field field, Literal value) {
    this.column = new Column(field.toString());
    this.predicateValue = value;
  }

  @Override
  public boolean apply(MultimapQueryResult input) {
    Collection<SValue> values = input.get(column);
    for (SValue value : values) {
      if (value.value().equals(predicateValue.toString()))
        return true;
    }

    return false;
  }

}
