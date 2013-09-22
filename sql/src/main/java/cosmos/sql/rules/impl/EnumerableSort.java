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

import net.hydromatic.linq4j.expressions.BlockBuilder;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.rules.java.EnumerableRel;
import net.hydromatic.optiq.rules.java.EnumerableRelImplementor;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

import cosmos.sql.AccumuloRel;
import cosmos.sql.AccumuloTable;

public class EnumerableSort extends SingleRel implements AccumuloRel, EnumerableRel {

  private AccumuloTable<?> accumuloAccessor;

  private RexNode offset = null;
  private RexNode fetch = null;

  public EnumerableSort(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexNode offset, RexNode fetch, AccumuloTable<?> accumuloAccessor) {
    super(cluster, traits, input);

    this.offset = offset;
    this.fetch = fetch;
    this.accumuloAccessor = accumuloAccessor;
  }

  @Override
  public SingleRel copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new EnumerableSort(getCluster(), traitSet, sole(inputs), fetch, offset, accumuloAccessor);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(0.1);
  }

  @Override
  public int implement(Plan implementor) {

    implementor.visitChild(getChild());

    implementor.table = accumuloAccessor;

    return 1;
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {

    final EnumerableRel child = (EnumerableRel) getChild();

    final Result result = implementor.visitChild(this, 0, child, pref);
    return result;
  }

}
//
