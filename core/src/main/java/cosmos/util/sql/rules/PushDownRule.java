package cosmos.util.sql.rules;

import java.util.UUID;

import org.eigenbase.rel.FilterRel;
import org.eigenbase.rel.ProjectRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.relopt.RelTraitSet;

import cosmos.util.sql.AccumuloRel;
import cosmos.util.sql.impl.CosmosTable;
import cosmos.util.sql.rules.impl.Filter;
import cosmos.util.sql.rules.impl.Projection;

public class PushDownRule extends RuleBase {

	CosmosTable accumuloAccessor;


	public PushDownRule(CosmosTable resultTable,RelOptRuleOperand operand) {
		super(operand,PushDownRule.class.getSimpleName() + UUID.randomUUID().toString());
		this.accumuloAccessor = resultTable;
	}
	
	
	@Override
	public void onMatch(RelOptRuleCall call) {
		
		RelNode node = call.getRels()[0];
		
		System.out.println(node.getClass() + " size of rule " + call.getRelList().size());
		if (node instanceof ProjectRel) {
			System.out.println("project");
			final ProjectRel project = (ProjectRel) node;
			final RelNode input = call.getRels()[1];
			final RelTraitSet traits = project.getTraitSet().plus(
					AccumuloRel.CONVENTION);
			final RelNode convertedInput = convert(input, traits);
			call.transformTo(new Projection(project.getCluster(), traits,
					convertedInput, project.getProjects(), project.getRowType(),accumuloAccessor));

		} else if (node instanceof FilterRel ) {
			System.out.println("filter");
			final FilterRel filter = (FilterRel)node;
			final RelNode input = call.getRels()[1];
			final RelTraitSet traits = filter.getTraitSet().plus(
					AccumuloRel.CONVENTION);
			final RelNode convertedInput = convert(input, traits);
			call.transformTo(new Filter(filter.getCluster(), traits,
					convertedInput, filter.getCondition(),accumuloAccessor));
		}
		else
		{
			System.out.println("shit");
		}

	}

	

}