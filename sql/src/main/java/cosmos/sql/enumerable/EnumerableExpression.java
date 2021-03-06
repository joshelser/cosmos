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
package cosmos.sql.enumerable;

import net.hydromatic.optiq.rules.java.EnumerableConvention;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;

import cosmos.sql.CosmosRelNode;

public class EnumerableExpression extends ConverterRule {
  public static final EnumerableExpression ARRAY_INSTANCE = new EnumerableExpression();
  
  private EnumerableExpression() {
    super(RelNode.class, CosmosRelNode.CONVENTION, EnumerableConvention.INSTANCE, "EnumerableRule");
    
  }
  
  @Override
  public boolean isGuaranteed() {
    return true;
  }
  
  @Override
  public RelNode convert(RelNode rel) {
    assert rel.getTraitSet().contains(CosmosRelNode.CONVENTION);
    return new EnumerableRelation(rel.getCluster(), rel.getTraitSet().replace(getOutConvention()), rel);
  }
}
