package cosmos.util.sql;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;

import cosmos.util.sql.call.CallIfc;
import cosmos.util.sql.call.BaseVisitor;

public interface AccumuloRel extends RelNode {
	/**
	 * Calling convention for relational expressions that are "implemented" by
	 * generating Drill logical plans.
	 */
	Convention CONVENTION = new Convention.Impl("COSMOS", AccumuloRel.class);

	int implement(Plan implementor);

	class Plan {

		protected BaseVisitor<CallIfc<?>> operations;

		public AccumuloTable<?> table;

		public Plan() {
			operations = new BaseVisitor<CallIfc<?>>();
		}

		/**
		 * Convenience method
		 * 
		 * @param operation
		 */
		public void add(String id, CallIfc<?> operation) {

			operations.addChild(id, operation);
		}

		public void visitChild(RelNode input) {
			((AccumuloRel) input).implement(this);

		}

		public BaseVisitor<CallIfc<?>> getChildren() {
			return operations;
		}
	}
}
