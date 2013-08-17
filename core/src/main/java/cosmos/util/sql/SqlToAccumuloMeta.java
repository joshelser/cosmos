package cosmos.util.sql;

import java.util.ArrayList;
import java.util.List;

import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.rex.RexNode;

import cosmos.util.sql.impl.CosmosTable;

public class SqlToAccumuloMeta extends RelOptRule {

	public static final RelOptRule INSTANCE = new SqlToAccumuloMeta();

	private SqlToAccumuloMeta() {
		// super(any(AccumuloRel.class));
		super(any(AccumuloRel.class));
	}

	@Override
	public void onMatch(RelOptRuleCall call) {

		System.out.println("wtf");
		System.exit(1);
		final TableScanner project = call.rel(0);

		final CosmosTable table = project.resultTable;

		final RelOptCluster cluster = project.getCluster();

		final ItemFinder itemFinder = new ItemFinder();
		final List<RexNode> newProjects = new ArrayList<RexNode>();
		System.out.println("children is " + project.getChildExps());
		for (RexNode rex : project.getChildExps()) {
			final RexNode rex2 = rex.accept(itemFinder);
			final RexNode rex3 = cluster.getRexBuilder().ensureType(
					rex.getType(), rex2, true);
			newProjects.add(rex3);
		}
		System.out.println("map=" + itemFinder.map + "\n projects="
				+ newProjects + "\n items=" + itemFinder.items);

		/*
		 * final List<Pair<String, String>> ops = new ArrayList<Pair<String,
		 * String>>(table.ops); final String findString =
		 * Util.toString(itemFinder.items, "{", ", ", "}"); final String
		 * aggregateString = "{$project ...}"; ops.add(Pair.of(findString,
		 * aggregateString)); final RelDataTypeFactory typeFactory =
		 * cluster.getTypeFactory(); final RelDataType rowType =
		 * typeFactory.createStructType(itemFinder.builder); final
		 * MongoTableScan newTable = new MongoTableScan(cluster,
		 * table.getTraitSet(), table.getTable(), table.mongoTable, rowType,
		 * ops); final ProjectRel newProject = new ProjectRel(cluster, newTable,
		 * newProjects, project.getRowType(), ProjectRel.Flags.Boxed,
		 * Collections.<RelCollation>emptyList());
		 */
		System.out.println("shit " + call.rel(0).getClass());

	}

}