package cosmos.util.sql;

import java.util.List;

import org.eigenbase.rel.FilterRel;
import org.eigenbase.rel.ProjectRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanWriter;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;

import com.google.common.collect.Lists;

import cosmos.util.sql.AccumuloRel.Implementor.IMPLEMENTOR_TYPE;
import cosmos.util.sql.call.Field;
import cosmos.util.sql.enumerable.EnumerableExpression;
import cosmos.util.sql.enumerable.FieldPacker;
import cosmos.util.sql.impl.CosmosTable;
import cosmos.util.sql.rules.PushDownRule;

/**
 * Enables the rules to scan a given accumulo table.
 * @author phrocker
 *
 */

public class TableScanner extends TableAccessRelBase implements AccumuloRel {
	final CosmosTable resultTable;

	final List<String> fieldList;
	
	List<String> selectedFields;

	public TableScanner(RelOptCluster cluster, RelTraitSet traitSet,
			RelOptTable table, CosmosTable resultTable, List<String> fieldList) {
		super(cluster, traitSet, table);

		this.resultTable = resultTable;
		this.fieldList = fieldList;
	}

	@Override
	public RelOptPlanWriter explainTerms(RelOptPlanWriter pw) {
		return super.explainTerms(pw);
	}

	@Override
	public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		assert inputs.isEmpty();
		return this;
	}

	public void setFields(List<String> fields)
	{
		selectedFields = Lists.newArrayList( fields );
	}
	
	@Override
	public void register(RelOptPlanner planner) {
		planner.addRule(new FieldPacker(this));
		planner.addRule(EnumerableExpression.ARRAY_INSTANCE);
		
		
		
		planner.addRule(new PushDownRule(resultTable,
	              new RelOptRuleOperand(
	                  FilterRel.class,
	                      new RelOptRuleOperand(TableScanner.class))));
		

		
		planner.addRule(new PushDownRule(resultTable,new RelOptRuleOperand(
	                  FilterRel.class,
	                  new RelOptRuleOperand(
	                      ProjectRel.class,
	                      new RelOptRuleOperand(TableScanner.class)))));
		
		

		
		planner.addRule(new PushDownRule(resultTable,
	                  new RelOptRuleOperand(
	                      ProjectRel.class,
	                      new RelOptRuleOperand(TableScanner.class))));
		
		planner.addRule(new PushDownRule(resultTable,new RelOptRuleOperand(
	              ProjectRel.class,
	              new RelOptRuleOperand(
	                  FilterRel.class,
	                  new RelOptRuleOperand(
	                      ProjectRel.class,
	                      new RelOptRuleOperand(TableScanner.class))))));
		

		

		
		
		
		
	}

	@Override
	public RelDataType deriveRowType() {

		return rowType != null ? rowType : super.deriveRowType();
	}




	@Override
	public RelOptCost computeSelfCost(RelOptPlanner planner) {
		// scans with a small project list are cheaper
		final float f = rowType == null ? 1f
				: (float) rowType.getFieldCount() / 100f;
		return super.computeSelfCost(planner).multiplyBy(.1 * f);
	}

	@Override
	public int implement(Implementor implementor) {
		implementor.table = resultTable;
		/**
		 * @TODO: woops, need to fix this
		 */

		implementor.add(IMPLEMENTOR_TYPE.SELECT, new Field(selectedFields.get(0)));
		
		return 0;
	}

}

