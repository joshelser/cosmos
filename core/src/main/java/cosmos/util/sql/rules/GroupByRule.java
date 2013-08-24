package cosmos.util.sql.rules;

import net.hydromatic.optiq.rules.java.EnumerableConvention;

import org.eigenbase.rel.AggregateRel;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelTraitSet;

import cosmos.util.sql.enumerable.EnumerableRelation;
import cosmos.util.sql.impl.CosmosTable;
import cosmos.util.sql.rules.impl.GroupBy;

/**
 * Initially the rules were separate; however, since they can be handled in a
 * single class we simply use this class to push the rules down the optimizer
 * 
 * @author phrocker
 * 
 */
public class GroupByRule extends ConverterRule {

	CosmosTable accumuloAccessor;

	public GroupByRule(CosmosTable resultTable) {
		// when see an aggregate who has a child operand
		// super(resultTable,some(AggregateRel.class, Convention.NONE,
		// any(RelNode.class)));
		super(AggregateRel.class, Convention.NONE,
				EnumerableConvention.INSTANCE, "EnumerableAggregateRulsdfse");
		this.accumuloAccessor = resultTable;
	}

	@Override
	public boolean isGuaranteed() {
		return true;
	}

	@Override
	public RelNode convert(RelNode rel) {
		
		if (rel instanceof AggregateRel) {
			final AggregateRel agg = (AggregateRel) rel;
			final RelTraitSet traitSet = agg.getTraitSet().replace(
					EnumerableConvention.INSTANCE);
			try {
				return new GroupBy(rel.getCluster(), traitSet, convert(
						agg.getChild(), traitSet), agg.getGroupSet(),
						agg.getAggCallList(), accumuloAccessor);
			} catch (InvalidRelException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		} else
			return new EnumerableRelation(rel.getCluster(), rel.getTraitSet()
					.replace(getOutConvention()), rel);
	}

}