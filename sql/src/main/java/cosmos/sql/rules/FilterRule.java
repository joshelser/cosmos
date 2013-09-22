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
package cosmos.sql.rules;

import org.eigenbase.rel.FilterRel;

import cosmos.sql.TableScanner;
import cosmos.sql.impl.CosmosTable;

/**
 * Initially the rule were separate; however, since they can be handled in a single class we simply use this class to push the rules down the optimizer
 * 
 * 
 */
public class FilterRule extends PushDownRule {
  
  CosmosTable accumuloAccessor;
  
  public FilterRule(CosmosTable resultTable) {
    super(resultTable, some(FilterRel.class, any(TableScanner.class)), "FilterShmilter");
    this.accumuloAccessor = resultTable;
  }
  
}
