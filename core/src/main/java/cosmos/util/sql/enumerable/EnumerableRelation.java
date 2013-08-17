/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package cosmos.util.sql.enumerable;

import java.util.List;

import net.hydromatic.linq4j.expressions.BlockBuilder;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.EnumerableRel;
import net.hydromatic.optiq.rules.java.EnumerableRelImplementor;
import net.hydromatic.optiq.rules.java.JavaRowFormat;
import net.hydromatic.optiq.rules.java.PhysType;
import net.hydromatic.optiq.rules.java.PhysTypeImpl;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cosmos.util.sql.AccumuloRel;
import cosmos.util.sql.SelectQuery;


public class EnumerableRelation extends SingleRel implements EnumerableRel {
	private static final Logger LOG = LoggerFactory
			.getLogger(EnumerableRelation.class);

	private static final Function1<String, Expression> TO_LITERAL = new Function1<String, Expression>() {
		@Override
		public Expression apply(String a0) {
			System.out.println("a0 is " + a0);
			return Expressions.constant(a0);
		}
	};

	private PhysType physType;

	public EnumerableRelation(RelOptCluster cluster, RelTraitSet traitSet,
			RelNode input) {
		
		super(cluster, traitSet, input);
		
		assert getConvention() instanceof EnumerableConvention;
		assert input.getConvention() == AccumuloRel.CONVENTION;
		physType = PhysTypeImpl.of((JavaTypeFactory) cluster.getTypeFactory(),
				input.getRowType(), JavaRowFormat.ARRAY);

	}

	public PhysType getPhysType() {
		return physType;
	}

	@Override
	public RelOptCost computeSelfCost(RelOptPlanner planner) {
		return super.computeSelfCost(planner).multiplyBy(.1);
	}

	@Override
	public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		return new EnumerableRelation(getCluster(), traitSet, sole(inputs));
	}

	public Result implement(EnumerableRelImplementor implementor, Prefer pref) {

		final BlockBuilder list = new BlockBuilder();

		SelectQuery selectQuery = new SelectQuery();
		
		System.out.println("Child is " + getChild().getClass());
		selectQuery.visitChild(0, getChild());
		
		
		final Expression table = list.append("table",
				selectQuery.table.getExpression());

		
		selectQuery.table.query(selectQuery);
		//gson.
		
		final Expression calls = list.append("calls", Expressions.constant(
				selectQuery));

		Expression accumuloResults = list.append("enumerable",
				Expressions.call(table, "accumulate"));

		
		list.add(Expressions.return_(null, accumuloResults));
		final PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(),
				getRowType(), pref.prefer(JavaRowFormat.ARRAY));
		return implementor.result(physType, list.toBlock());

	
	}

}
