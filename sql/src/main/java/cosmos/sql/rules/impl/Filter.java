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

import java.util.List;

import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

import cosmos.sql.CosmosRelNode;
import cosmos.sql.DataTable;
import cosmos.sql.call.CallIfc;
import cosmos.sql.call.impl.OperationVisitor;

public class Filter extends FilterRelBase implements CosmosRelNode {
  
  private DataTable<?> accumuloAccessor;
  
  public Filter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition, DataTable<?> accumuloAccessor) {
    super(cluster, traits, child, condition);
    
    assert getConvention() == CONVENTION;
    this.accumuloAccessor = accumuloAccessor;
  }
  
  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new Filter(getCluster(), traitSet, sole(inputs), getCondition(), accumuloAccessor);
  }
  
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
	  /**
	   * At this point we don't have schema statistics
	   */
    return super.computeSelfCost(planner).multiplyBy(0.1);
  }
  
  @Override
  public int implement(Plan implementor) {
    
    implementor.visitChild(getChild());
    
    OperationVisitor visitor = new OperationVisitor(getChild());
    CallIfc<?> operation = getCondition().accept(visitor);
    
    cosmos.sql.call.impl.Filter filter = new cosmos.sql.call.impl.Filter();
    
    filter.addChild(operation.getClass().getSimpleName(), operation);
    implementor.add(filter.getClass().getSimpleName(), filter);
    implementor.table = accumuloAccessor;
    
    return 1;
  }
  
}
