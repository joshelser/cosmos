package cosmos.util.sql.enumerable;

import net.hydromatic.optiq.rules.java.EnumerableRelImplementor;
import net.hydromatic.optiq.rules.java.JavaRules.EnumerableCalcRel;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptPlanWriter;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexProgram;

public class FieldPackerRelation extends EnumerableCalcRel {

	public FieldPackerRelation(RelOptCluster cluster, RelTraitSet traitSet,
			RelNode child, RexProgram program, int flags) {
		super(cluster, traitSet, child, program, flags);
		System.out.println("okay maybe 1");
	}
	

	@Override
    public RelOptPlanWriter explainTerms(RelOptPlanWriter pw) {
    	System.out.println("explainny");
      return getProgram().explainCalc(super.explainTerms(pw));
    }

	@Override
	public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
		System.out.println("okay maybe 2");
		Result result =  super.implement(implementor, pref);
		
	
		
		return result;
	}

}
