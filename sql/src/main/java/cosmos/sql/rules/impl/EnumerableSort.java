package cosmos.sql.rules.impl;

import java.util.List;

import net.hydromatic.linq4j.expressions.BlockBuilder;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.rules.java.EnumerableRel;
import net.hydromatic.optiq.rules.java.EnumerableRelImplementor;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

import cosmos.sql.AccumuloRel;
import cosmos.sql.AccumuloTable;

public class EnumerableSort extends SingleRel implements AccumuloRel, EnumerableRel {
  
  private AccumuloTable<?> accumuloAccessor;
  
  private RexNode offset = null;
  private RexNode fetch = null;
  
  public EnumerableSort(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexNode offset, RexNode fetch, AccumuloTable<?> accumuloAccessor) {
    super(cluster, traits, input);
    System.out.println("sort?");
    this.offset = offset;
    this.fetch = fetch;
    // assert getConvention() == CONVENTION;
    // assert getConvention() == input.getConvention();
    System.out.println("sort");
    this.accumuloAccessor = accumuloAccessor;
  }
  
  @Override
  public SingleRel copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new EnumerableSort(getCluster(), traitSet, sole(inputs), fetch, offset, accumuloAccessor);
  }
  
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(0.1);
  }
  
  @Override
  public int implement(Plan implementor) {
    
    System.out.println("we got limit");
    implementor.visitChild(getChild());
    
    implementor.table = accumuloAccessor;
    
    return 1;
  }
  
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    
    final JavaTypeFactory typeFactory = implementor.getTypeFactory();
    final BlockBuilder builder = new BlockBuilder();
    final EnumerableRel child = (EnumerableRel) getChild();
    
    final Result result = implementor.visitChild(this, 0, child, pref);
    System.out.println("muffaka");
    return result;
  }
  
}
//
