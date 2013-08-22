package cosmos.util.sql.rules.impl;

import java.util.List;

import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

import cosmos.util.sql.AccumuloRel;
import cosmos.util.sql.AccumuloRel.Planner;
import cosmos.util.sql.AccumuloRel.Planner.IMPLEMENTOR_TYPE;
import cosmos.util.sql.AccumuloTable;
import cosmos.util.sql.call.CallIfc;
import cosmos.util.sql.call.OperationVisitor;
/**
 * Filter rule
 * 
 * @author phrocker
 *
 */
public class Filter extends FilterRelBase implements AccumuloRel {

	private AccumuloTable accumuloAccessor;

	public Filter(RelOptCluster cluster, RelTraitSet traits,
			RelNode child, RexNode condition, AccumuloTable accumuloAccessor) {
		super(cluster, traits, child, condition);
		
		assert getConvention() == CONVENTION;
		this.accumuloAccessor = accumuloAccessor;
	}

	@Override
	public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		return new Filter(getCluster(), traitSet, sole(inputs),
				getCondition(),accumuloAccessor);
	}

	@Override
	public RelOptCost computeSelfCost(RelOptPlanner planner) {
		return super.computeSelfCost(planner).multiplyBy(0.1);
	}

	@Override
	public int implement(Planner implementor) {

		
		
		implementor.visitChild(0, getChild());
		
		System.out.println("filter " + getChild().getDescription());
		
		OperationVisitor visitor = new OperationVisitor(getChild());
		CallIfc operation = getCondition().accept(visitor);
		
		cosmos.util.sql.call.impl.Filter filter = new cosmos.util.sql.call.impl.Filter();
		filter.addChild(operation);
		implementor.add(filter);
		implementor.table = accumuloAccessor; 

		return 1;
	}

}