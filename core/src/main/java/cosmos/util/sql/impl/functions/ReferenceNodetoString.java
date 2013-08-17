package cosmos.util.sql.impl.functions;


import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;

import com.google.common.base.Function;

public class ReferenceNodetoString implements Function<RexNode,String> {

	@Override
	public
	String apply(RexNode exp) {
		
		if (exp instanceof RexInputRef) {
			System.out.println("node " + ((RexInputRef) exp).getIndex());
			return ((RexInputRef) exp).getName();
		}
		else
			return exp.toString();
	}

}
