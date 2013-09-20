package cosmos.sql.rules;

import net.hydromatic.optiq.rules.java.EnumerableConvention;

import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SortRel;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;

import cosmos.sql.AccumuloRel;
import cosmos.sql.impl.CosmosTable;
import cosmos.sql.rules.impl.EnumerableSort;

/**
 * Initially the rule were separate; however, since they can be handled in a single class we simply use this class to push the rules down the optimizer
 * 
 * @author phrocker
 * 
 */
public class LimitRule extends ConverterRule {
  
  CosmosTable accumuloAccessor;
  
  public LimitRule(CosmosTable resultTable) {
    // super(any(SortRel.class), "SorterShmorter");
    super(SortRel.class, Convention.NONE, AccumuloRel.CONVENTION, "SorterShmorter");
    this.accumuloAccessor = resultTable;
  }
  
  @Override
  public void onMatch(RelOptRuleCall call) {
    
    final SortRel sort = (SortRel) call.rel(0);
    
    // do not handle other rules
    if (sort.offset == null && sort.fetch == null) {
      
      return;
    }
    System.out.println("limit " + sort.fetch + " " + sort.offset);
    System.out.println("sort rel ");
    
    final RelTraitSet traits = sort.getTraitSet().plus(AccumuloRel.CONVENTION);
    
    RelNode input = sort.getChild();
    if (!sort.getCollation().getFieldCollations().isEmpty()) {
      input = sort.copy(sort.getTraitSet().replace(RelCollationImpl.EMPTY), input, RelCollationImpl.EMPTY, null, null);
    }
    
    final RelNode convertedInput = convert(input, input.getTraitSet().plus(AccumuloRel.CONVENTION));
    
    call.transformTo(new EnumerableSort(sort.getCluster(), traits, convertedInput, sort.fetch, sort.offset, accumuloAccessor));
    
  }
  
  @Override
  public RelNode convert(RelNode rel) {
    final SortRel sort = (SortRel) rel;
    
    // do not handle other rules
    if (sort.offset == null && sort.fetch == null) {
      
      return sort;
    }
    System.out.println("limit " + sort.fetch + " " + sort.offset);
    System.out.println("sort rel ");
    
    final RelTraitSet traits = sort.getTraitSet().plus(EnumerableConvention.INSTANCE);
    
    RelNode input = sort.getChild();
    if (!sort.getCollation().getFieldCollations().isEmpty()) {
      input = sort.copy(sort.getTraitSet().replace(RelCollationImpl.EMPTY), input, RelCollationImpl.EMPTY, null, null);
    }
    
    final RelNode convertedInput = convert(input, input.getTraitSet().plus(EnumerableConvention.INSTANCE));
    
    return new EnumerableSort(sort.getCluster(), traits, convertedInput, sort.fetch, sort.offset, accumuloAccessor);
  }
  
}
