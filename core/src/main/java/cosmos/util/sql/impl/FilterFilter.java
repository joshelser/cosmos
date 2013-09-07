package cosmos.util.sql.impl;

import com.google.common.base.Predicate;
import com.google.gson.JsonSerializer;

import cosmos.util.sql.call.ChildVisitor;
import cosmos.util.sql.call.impl.FieldEquality;

public class FilterFilter implements Predicate<ChildVisitor> {

	@Override
	public boolean apply(ChildVisitor input) {
		if (input instanceof FieldEquality)
			return true;
		else
			return false;
	}

}
