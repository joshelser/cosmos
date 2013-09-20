package cosmos.sql.rules.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;

import cosmos.sql.AccumuloRel;
import cosmos.sql.AccumuloTable;
import cosmos.sql.call.CallIfc;
import cosmos.sql.call.impl.OperationVisitor;

/**
 * Projection based rule
 * 
 * @author phrocker
 * 
 */
public class Projection extends ProjectRelBase implements AccumuloRel {
  
  private AccumuloTable<?> accumuloAccessor;
  
  public Projection(RelOptCluster cluster, RelTraitSet traits, RelNode child, List<RexNode> exps, RelDataType rowType, AccumuloTable<?> accumuloAccessor) {
    super(cluster, traits, child, exps, rowType, Flags.Boxed, Collections.<RelCollation> emptyList());
    assert getConvention() == CONVENTION;
    this.accumuloAccessor = accumuloAccessor;
  }
  
  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new Projection(getCluster(), traitSet, sole(inputs), new ArrayList<RexNode>(exps), rowType, accumuloAccessor);
  }
  
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(0.1);
  }
  
  @Override
  public int implement(Plan implementor) {
    
    implementor.visitChild(getChild());
    
    implementor.table = accumuloAccessor;
    
    OperationVisitor visitor = new OperationVisitor(getChild());
    
    cosmos.sql.call.impl.Projection projections = new cosmos.sql.call.impl.Projection();
    for (RexNode node : exps) {
      CallIfc projection = node.accept(visitor);
      projections.addChild(projection.getClass().getSimpleName(), projection);
      
    }
    implementor.add(projections.getClass().getSimpleName(), projections);
    
    return 1;
    
  }
  
}
