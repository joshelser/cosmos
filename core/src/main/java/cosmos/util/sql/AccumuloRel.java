package cosmos.util.sql;

import java.util.Map;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;

import com.google.common.collect.Maps;

import cosmos.util.sql.call.CallIfc;


public interface AccumuloRel extends RelNode {
	/**
	 * Calling convention for relational expressions that are "implemented" by
	 * generating Drill logical plans.
	 */
	Convention CONVENTION = new Convention.Impl("COSMOS", AccumuloRel.class);

	int implement(Implementor implementor);

	class Implementor {
		public enum IMPLEMENTOR_TYPE
		{
			PROJECTION,
			SELECT,
			FILTER,
			OTHER;
		}
		final protected Map<IMPLEMENTOR_TYPE,CallIfc> operations = Maps.newHashMap();

		public AccumuloTable table;

		public void add(IMPLEMENTOR_TYPE type, CallIfc operation) {
			
			System.out.println("adding " + operations.size());
			operations.put(type,operation);
		}

		public void visitChild(int ordinal, RelNode input) {
			assert ordinal == 0;
			System.out.println("visit " + input.getClass() );

			((AccumuloRel) input).implement(this);

		}
	}
}
