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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;

import cosmos.sql.AccumuloRel;
import cosmos.sql.AccumuloTable;
import cosmos.sql.call.CallIfc;
import cosmos.sql.call.impl.OperationVisitor;

/**
 * Projection based rule
 * 
 * 
 */
public class Projection extends ProjectRelBase implements AccumuloRel {
  
  private AccumuloTable<?> accumuloAccessor;
  
  public Projection(RelOptCluster cluster, RelTraitSet traits, RelNode child, List<RexNode> exps, RelDataType rowType, AccumuloTable<?> accumuloAccessor) {
    super(cluster, traits, child, exps, rowType, Flags.Boxed, Collections.<RelCollation> emptyList());
    assert getConvention() == CONVENTION;
    this.accumuloAccessor = accumuloAccessor;
  }
  
  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new Projection(getCluster(), traitSet, sole(inputs), new ArrayList<RexNode>(exps), rowType, accumuloAccessor);
  }
  
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(0.1);
  }
  
  @Override
  public int implement(Plan implementor) {
    
    implementor.visitChild(getChild());
    
    implementor.table = accumuloAccessor;
    
    OperationVisitor visitor = new OperationVisitor(getChild());
    
    cosmos.sql.call.impl.Projection projections = new cosmos.sql.call.impl.Projection();
    for (RexNode node : exps) {
      CallIfc<?> projection = node.accept(visitor);
      projections.addChild(projection.getClass().getSimpleName(), projection);
      
    }
    implementor.add(projections.getClass().getSimpleName(), projections);
    
    return 1;
    
  }
  
}
