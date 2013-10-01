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
package cosmos.sql.rules.impl;

import java.util.BitSet;
import java.util.List;

import net.hydromatic.linq4j.expressions.BlockBuilder;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.EnumerableRel;
import net.hydromatic.optiq.rules.java.EnumerableRelImplementor;
import net.hydromatic.optiq.rules.java.JavaRules.EnumerableAggregateRel;
import net.hydromatic.optiq.rules.java.PhysType;
import net.hydromatic.optiq.rules.java.PhysTypeImpl;

import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;

import cosmos.sql.CosmosRelNode;
import cosmos.sql.DataTable;
import cosmos.sql.call.Field;

public class GroupBy extends EnumerableAggregateRel implements CosmosRelNode {
  
  private DataTable<?> accumuloAccessor;
  
  public GroupBy(RelOptCluster cluster, RelTraitSet traits, RelNode input, BitSet groupSet, List<AggregateCall> list, DataTable<?> accumuloAccessor)
      throws InvalidRelException {
    super(cluster, traits, input, groupSet, list);
    assert getConvention() instanceof EnumerableConvention;
    this.accumuloAccessor = accumuloAccessor;
  }
  
  @Override
  public EnumerableAggregateRel copy(RelTraitSet traitSet, List<RelNode> inputs) {
    try {
      return new GroupBy(getCluster(), traitSet, sole(inputs), groupSet, getAggCallList(), accumuloAccessor);
    } catch (InvalidRelException e) {
      e.printStackTrace();
    }
    
    return null;
  }
  
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(0.1);
  }
  
  @Override
  public int implement(Plan implementor) {
    
    return 1;
  }
  
  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    
    final JavaTypeFactory typeFactory = implementor.getTypeFactory();
    final BlockBuilder builder = new BlockBuilder();
    final EnumerableRel child = (EnumerableRel) getChild();
    
    Plan aggregatePlan = new Plan();
    
    List<String> fieldName = child.getRowType().getFieldNames();
    
    for (int i = 0; i < getGroupCount(); i++) {
      if (getGroupSet().get(i)) {
        aggregatePlan.add("groupBy", new Field(fieldName.get(i)));
      }
    }
    
    accumuloAccessor.groupBy(aggregatePlan);
    
    final Result result = implementor.visitChild(this, 0, child, pref);
    
    Expression childExp = builder.append("child", result.block);
    
    final PhysType physType = PhysTypeImpl.of(typeFactory, getRowType(), pref.preferCustom());
    
    builder.add(Expressions.return_(null, childExp));
    
    return implementor.result(physType, builder.toBlock());
  }
  
}
//
