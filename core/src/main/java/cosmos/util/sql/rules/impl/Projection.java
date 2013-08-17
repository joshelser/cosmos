package cosmos.util.sql.rules.impl;

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

import cosmos.util.sql.AccumuloRel;
import cosmos.util.sql.AccumuloRel.Implementor.IMPLEMENTOR_TYPE;
import cosmos.util.sql.AccumuloTable;
import cosmos.util.sql.call.CallIfc;
import cosmos.util.sql.call.OperationVisitor;

public class Projection extends ProjectRelBase implements AccumuloRel {

	private AccumuloTable accumuloAccessor;

	public Projection(RelOptCluster cluster, RelTraitSet traits,
			RelNode child, List<RexNode> exps, RelDataType rowType, AccumuloTable accumuloAccessor) {
		super(cluster, traits, child, exps, rowType, Flags.Boxed,
				Collections.<RelCollation> emptyList());
		assert getConvention() == CONVENTION;
		this.accumuloAccessor = accumuloAccessor;
	}

	@Override
	public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		return new Projection(getCluster(), traitSet, sole(inputs),
				new ArrayList<RexNode>(exps), rowType,accumuloAccessor);
	}

	@Override
	public RelOptCost computeSelfCost(RelOptPlanner planner) {
		return super.computeSelfCost(planner).multiplyBy(0.1);
	}

	@Override
	public int implement(Implementor implementor) {

		AccumuloRel.Implementor parent = new AccumuloRel.Implementor();
		
		parent.visitChild(0, getChild());
		
		implementor.table = accumuloAccessor;

		OperationVisitor visitor = new OperationVisitor(getChild());

		cosmos.util.sql.call.impl.Projection projections = new cosmos.util.sql.call.impl.Projection();
		for (RexNode node : exps) {
			CallIfc projection = node.accept(visitor);
			projections.addChild(projection);

		}
		implementor.add(IMPLEMENTOR_TYPE.PROJECTION, projections);

		return 1;

	}

}
