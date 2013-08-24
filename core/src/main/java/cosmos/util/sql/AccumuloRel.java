package cosmos.util.sql;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;

import cosmos.util.sql.call.CallIfc;
import cosmos.util.sql.call.ChildVisitor;

public interface AccumuloRel extends RelNode {
	/**
	 * Calling convention for relational expressions that are "implemented" by
	 * generating Drill logical plans.
	 */
	Convention CONVENTION = new Convention.Impl("COSMOS", AccumuloRel.class);

	int implement(Plan implementor);

	class Plan {

		protected ChildVisitor<CallIfc<?>> operations;

		public AccumuloTable<?> table;

		public Plan() {
			operations = new ChildVisitor<CallIfc<?>>();
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
			System.out.println("rel " +  input.getClass());
			((AccumuloRel) input).implement(this);

		}

		public ChildVisitor<CallIfc<?>> getChildren() {
			return operations;
		}
	}
}
