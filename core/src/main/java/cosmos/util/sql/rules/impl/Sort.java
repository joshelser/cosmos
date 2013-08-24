package cosmos.util.sql.rules.impl;

import java.util.List;

import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SortRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;

import cosmos.util.sql.AccumuloRel;
import cosmos.util.sql.AccumuloTable;

public class Sort extends SortRel implements AccumuloRel {

	private AccumuloTable<?> accumuloAccessor;

	public Sort(RelOptCluster cluster, RelTraitSet traits,
			RelNode input, RelCollation collation, AccumuloTable<?> accumuloAccessor) {
		super(cluster, traits, input, collation);
		assert getConvention() == CONVENTION;
		this.accumuloAccessor = accumuloAccessor;
	}

	@Override
	public SortRel copy(RelTraitSet traitSet, List<RelNode> inputs) {
		return new Sort(getCluster(), traitSet, sole(inputs),
				getCollation(),accumuloAccessor);
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

}
//