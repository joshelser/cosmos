package cosmos.sql.rules;

import org.apache.log4j.Logger;
import org.eigenbase.rel.AggregateRel;
import org.eigenbase.rel.FilterRel;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.ProjectRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SortRel;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.relopt.RelTraitSet;

import cosmos.sql.AccumuloRel;
import cosmos.sql.impl.CosmosTable;
import cosmos.sql.rules.impl.Filter;
import cosmos.sql.rules.impl.GroupBy;
import cosmos.sql.rules.impl.Projection;
import cosmos.sql.rules.impl.EnumerableSort;

/**
 * Initially the rule were separate; however, since they can be handled in a single class we simply use this class to push the rules down the optimizer
 * 
 * @author phrocker
 * 
 */
public class PushDownRule extends RuleBase {
  
  CosmosTable accumuloAccessor;
  
  private static final Logger log = Logger.getLogger(PushDownRule.class);
  
  public PushDownRule(CosmosTable resultTable, RelOptRuleOperand operand, String name) {
    super(operand, PushDownRule.class.getSimpleName() + name);
    this.accumuloAccessor = resultTable;
  }
  
  @Override
  public void onMatch(RelOptRuleCall call) {
    
    RelNode node = call.rel(0);
    
    if (node instanceof ProjectRel) {
      final ProjectRel project = (ProjectRel) node;
      final RelNode input = call.rel(1);
      final RelTraitSet traits = project.getTraitSet().plus(AccumuloRel.CONVENTION);
      final RelNode convertedInput = convert(input, traits);
      call.transformTo(new Projection(project.getCluster(), traits, convertedInput, project.getProjects(), project.getRowType(), accumuloAccessor));
      
    } else if (node instanceof FilterRel) {
      final FilterRel filter = (FilterRel) node;
      
      final RelNode input = call.rel(1);
      final RelTraitSet traits = filter.getTraitSet().plus(AccumuloRel.CONVENTION);
      final RelNode convertedInput = convert(input, traits);
      call.transformTo(new Filter(filter.getCluster(), traits, convertedInput, filter.getCondition(), accumuloAccessor));
      
    } else if (node instanceof AggregateRel) {
      
      final AggregateRel aggy = (AggregateRel) node;
      final RelNode input = call.rel(1);
      
      final RelTraitSet traits = aggy.getTraitSet().plus(AccumuloRel.CONVENTION);
      
      final RelNode convertedInput = convert(input, traits);
      try {
        call.transformTo(new GroupBy(aggy.getCluster(), traits, convertedInput, aggy.getGroupSet(), aggy.getAggCallList(), accumuloAccessor));
      } catch (InvalidRelException e) {
        log.error(e);
      }
    } else {
      
    }
    
  }
  
}
