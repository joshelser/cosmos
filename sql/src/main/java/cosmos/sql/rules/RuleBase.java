package cosmos.sql.rules;

import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleOperand;

/**
 * Rule base
 * 
 * @author phrocker
 * 
 */
public abstract class RuleBase extends RelOptRule {
  
  public RuleBase(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }
  
}
