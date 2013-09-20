package cosmos.sql.enumerable;

import net.hydromatic.optiq.rules.java.EnumerableRelImplementor;
import net.hydromatic.optiq.rules.java.JavaRules.EnumerableCalcRel;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptPlanWriter;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexProgram;

public class FieldPackerRelation extends EnumerableCalcRel {
  
  public FieldPackerRelation(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexProgram program, int flags) {
    super(cluster, traitSet, child, program, flags);
  }
  
  @Override
  public RelOptPlanWriter explainTerms(RelOptPlanWriter pw) {
    return getProgram().explainCalc(super.explainTerms(pw));
  }
  
  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    
    Result result = super.implement(implementor, pref);
    
    return result;
  }
  
}
