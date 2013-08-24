package cosmos.util.sql.rules.impl;

import java.util.BitSet;
import java.util.List;

import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;

import cosmos.util.sql.AccumuloRel;
import cosmos.util.sql.AccumuloTable;

public class GroupBy extends AggregateRelBase implements AccumuloRel {

	private AccumuloTable<?> accumuloAccessor;

	public GroupBy(RelOptCluster cluster, RelTraitSet traits,
			RelNode input, BitSet groupSet, List<AggregateCall> list, AccumuloTable<?> accumuloAccessor) {
		super(cluster,traits,input,groupSet,list);
		assert getConvention() == CONVENTION;
		this.accumuloAccessor = accumuloAccessor;
	}

	@Override
	public AggregateRelBase copy(RelTraitSet traitSet, List<RelNode> inputs) {
		return new GroupBy(getCluster(), traitSet, sole(inputs), groupSet, getAggCallList(), accumuloAccessor);
	}

	@Override
	public RelOptCost computeSelfCost(RelOptPlanner planner) {
		return super.computeSelfCost(planner).multiplyBy(0.1);
	}

	@Override
	public int implement(Plan implementor) {

		System.exit(1);
		System.out.println("oh group by " );
		implementor.visitChild(getChild());
		
		
		System.out.println("oh group by " );
		for(AggregateCall call : getAggCallList())
		{
			System.out.println(call.getName() + " " + call.getAggregation().getName());
		}
		//Grouping grouping = new Grouping();
		
		
		//grouping.addChild(Field.class.getSimpleName(), new Field(literal))
		
		implementor.table = accumuloAccessor; 

		return 1;
	}

}
//