package cosmos.util.sql.rules;

import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleOperand;

public abstract class RuleBase extends RelOptRule{

	public RuleBase(RelOptRuleOperand operand, String description) {
		super(operand, description);
	}

	
}
