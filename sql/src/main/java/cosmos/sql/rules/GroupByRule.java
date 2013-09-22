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

import net.hydromatic.optiq.rules.java.EnumerableConvention;

import org.eigenbase.rel.AggregateRel;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelTraitSet;

import cosmos.sql.enumerable.EnumerableRelation;
import cosmos.sql.impl.CosmosTable;
import cosmos.sql.rules.impl.GroupBy;

/**
 * Initially the rules were separate; however, since they can be handled in a single class we simply use this class to push the rules down the optimizer
 * 
 * 
 * 
 */
public class GroupByRule extends ConverterRule {
  
  CosmosTable accumuloAccessor;
  
  public GroupByRule(CosmosTable resultTable) {
    // when see an aggregate who has a child operand
    // super(resultTable,some(AggregateRel.class, Convention.NONE,
    // any(RelNode.class)));
    super(AggregateRel.class, Convention.NONE, EnumerableConvention.INSTANCE, "EnumerableAggregateRulsdfse");
    this.accumuloAccessor = resultTable;
  }
  
  @Override
  public boolean isGuaranteed() {
    return true;
  }
  
  @Override
  public RelNode convert(RelNode rel) {
    
    if (rel instanceof AggregateRel) {
      final AggregateRel agg = (AggregateRel) rel;
      final RelTraitSet traitSet = agg.getTraitSet().replace(EnumerableConvention.INSTANCE);
      try {
        return new GroupBy(rel.getCluster(), traitSet, convert(agg.getChild(), traitSet), agg.getGroupSet(), agg.getAggCallList(), accumuloAccessor);
      } catch (InvalidRelException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      return null;
    } else
      return new EnumerableRelation(rel.getCluster(), rel.getTraitSet().replace(getOutConvention()), rel);
  }
  
}
