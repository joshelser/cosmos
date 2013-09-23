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

import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SortRel;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;

import cosmos.sql.CosmosRelNode;
import cosmos.sql.impl.CosmosTable;
import cosmos.sql.rules.impl.EnumerableSort;

/**
 * Initially the rule were separate; however, since they can be handled in a single class we simply use this class to push the rules down the optimizer
 * 
 * 
 */
public class LimitRule extends ConverterRule {

  CosmosTable accumuloAccessor;

  public LimitRule(CosmosTable resultTable) {
    super(SortRel.class, Convention.NONE, CosmosRelNode.CONVENTION, "SorterShmorter");
    this.accumuloAccessor = resultTable;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {

    final SortRel sort = (SortRel) call.rel(0);

    // do not handle other rules
    if (sort.offset == null && sort.fetch == null) {

      return;
    }

    final RelTraitSet traits = sort.getTraitSet().plus(CosmosRelNode.CONVENTION);

    RelNode input = sort.getChild();
    if (!sort.getCollation().getFieldCollations().isEmpty()) {
      input = sort.copy(sort.getTraitSet().replace(RelCollationImpl.EMPTY), input, RelCollationImpl.EMPTY, null, null);
    }

    final RelNode convertedInput = convert(input, input.getTraitSet().plus(CosmosRelNode.CONVENTION));

    call.transformTo(new EnumerableSort(sort.getCluster(), traits, convertedInput, sort.fetch, sort.offset, accumuloAccessor));

  }

  @Override
  public RelNode convert(RelNode rel) {
    final SortRel sort = (SortRel) rel;

    // do not handle other rules
    if (sort.offset == null && sort.fetch == null) {

      return sort;
    }

    final RelTraitSet traits = sort.getTraitSet().plus(EnumerableConvention.INSTANCE);

    RelNode input = sort.getChild();
    if (!sort.getCollation().getFieldCollations().isEmpty()) {
      input = sort.copy(sort.getTraitSet().replace(RelCollationImpl.EMPTY), input, RelCollationImpl.EMPTY, null, null);
    }

    final RelNode convertedInput = convert(input, input.getTraitSet().plus(EnumerableConvention.INSTANCE));

    return new EnumerableSort(sort.getCluster(), traits, convertedInput, sort.fetch, sort.offset, accumuloAccessor);
  }

}
