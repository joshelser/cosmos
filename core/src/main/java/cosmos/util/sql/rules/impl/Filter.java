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
import cosmos.util.sql.AccumuloRel.Implementor;
import cosmos.util.sql.AccumuloRel.Implementor.IMPLEMENTOR_TYPE;
import cosmos.util.sql.AccumuloTable;
import cosmos.util.sql.call.CallIfc;
import cosmos.util.sql.call.OperationVisitor;


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
	public int implement(Implementor implementor) {

		
		implementor.visitChild(0, getChild());
		
		OperationVisitor visitor = new OperationVisitor(getChild());
		CallIfc operation = getCondition().accept(visitor);
		System.out.println("buf " + operation.getClass());
		
		cosmos.util.sql.call.impl.Filter filter = new cosmos.util.sql.call.impl.Filter();
		filter.addChild(operation);
		implementor.add(IMPLEMENTOR_TYPE.FILTER,filter);
		implementor.table = accumuloAccessor; 

		return 1;
		// final int inputId = implementor.im(this, 0, getChild());
		// final ObjectNode filter = implementor.mapper.createObjectNode();
		/*
		 * E.g. { op: "filter", expr: "donuts.ppu < 1.00" }
		 * 
		 * filter.put("op", "filter"); filter.put("input", inputId);
		 * filter.put("expr", DrillOptiq.toDrill(getChild(),
		 * getCondition())); return implementor.add(filter);
		 */
	}

}