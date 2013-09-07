package cosmos.util.sql.impl;

import com.google.common.base.Predicate;

import cosmos.util.sql.call.ChildVisitor;
import cosmos.util.sql.call.impl.operators.AndOperator;

public class BinaryLogicFilter implements Predicate<ChildVisitor> {

	@Override
	public boolean apply(ChildVisitor input) {
		if (input instanceof AndOperator)
		{
			return true;
		}
		return false;
	}

	
}
