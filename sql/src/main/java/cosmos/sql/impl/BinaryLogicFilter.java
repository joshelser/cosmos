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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

import cosmos.sql.call.ChildVisitor;
import cosmos.sql.call.impl.operators.AndOperator;

public class BinaryLogicFilter implements Predicate<ChildVisitor> {
  
  @Override
  public boolean apply(ChildVisitor input) {
	  Preconditions.checkNotNull(input);
    if (input instanceof AndOperator) {
      return true;
    }
    return false;
  }
  
}
