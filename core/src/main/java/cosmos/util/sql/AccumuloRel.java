package cosmos.util.sql;

import java.util.Map;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;

import com.google.common.collect.Maps;

import cosmos.util.sql.call.CallIfc;
import cosmos.util.sql.call.Fields;
import cosmos.util.sql.call.impl.Filter;
import cosmos.util.sql.call.impl.Projection;
public interface AccumuloRel extends RelNode {
	/**
	 * Calling convention for relational expressions that are "implemented" by
	 * generating Drill logical plans.
	 */
	Convention CONVENTION = new Convention.Impl("COSMOS", AccumuloRel.class);

	int implement(Planner implementor);

	class Planner {
		public enum IMPLEMENTOR_TYPE {
			PROJECTION, FIELD_SELECT, FILTER, OTHER;
		}

		final protected Map<IMPLEMENTOR_TYPE, CallIfc> operations = Maps
				.newHashMap();

		public AccumuloTable table;

		/**
		 * Convenience method
		 * 
		 * @param operation
		 */
		public void add(CallIfc operation) {

			if (operation instanceof Fields) {
				operations.put(IMPLEMENTOR_TYPE.FIELD_SELECT, operation);
			}
			else if (operation instanceof Filter)
			{
				operations.put(IMPLEMENTOR_TYPE.FILTER,operation);
			}
			else if (operation instanceof Projection)
			{
				operations.put(IMPLEMENTOR_TYPE.PROJECTION,operation);
			}
		}

		public void visitChild(int ordinal, RelNode input) {
			assert ordinal == 0;
			((AccumuloRel) input).implement(this);

		}
	}
}
