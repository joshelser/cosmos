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

import net.hydromatic.optiq.rules.java.EnumerableRelImplementor;
import net.hydromatic.optiq.rules.java.JavaRules.EnumerableCalcRel;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptPlanWriter;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexProgram;

public class FieldPackerRelation extends EnumerableCalcRel {
  
  public FieldPackerRelation(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexProgram program, int flags) {
    super(cluster, traitSet, child, program, flags);
  }
  
  @Override
  public RelOptPlanWriter explainTerms(RelOptPlanWriter pw) {
    return getProgram().explainCalc(super.explainTerms(pw));
  }
  
  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    
    Result result = super.implement(implementor, pref);
    
    return result;
  }
  
}
