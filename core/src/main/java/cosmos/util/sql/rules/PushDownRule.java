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

/**
 * Initially the rule were separate; however, since 
 * they can be handled in a single class we simply use this class
 * to push the rules down the optimizer
 * @author phrocker
 *
 */
public class PushDownRule extends RuleBase {

	CosmosTable accumuloAccessor;


	public PushDownRule(CosmosTable resultTable,RelOptRuleOperand operand) {
		super(operand,PushDownRule.class.getSimpleName() + UUID.randomUUID().toString());
		this.accumuloAccessor = resultTable;
	}
	
	
	@Override
	public void onMatch(RelOptRuleCall call) {
		
		RelNode node = call.getRels()[0];
		
		if (node instanceof ProjectRel) {
			final ProjectRel project = (ProjectRel) node;
			final RelNode input = call.getRels()[1];
			final RelTraitSet traits = project.getTraitSet().plus(
					AccumuloRel.CONVENTION);
			final RelNode convertedInput = convert(input, traits);
			call.transformTo(new Projection(project.getCluster(), traits,
					convertedInput, project.getProjects(), project.getRowType(),accumuloAccessor));

		} else if (node instanceof FilterRel ) {
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

		}

	}

	

}