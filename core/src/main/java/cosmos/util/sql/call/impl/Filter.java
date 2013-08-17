package cosmos.util.sql.call.impl;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import cosmos.util.sql.call.CallIfc;
import cosmos.util.sql.call.ChildVisitor;
import cosmos.util.sql.call.FilterIfc;

public class Filter extends ChildVisitor {

	public Filter() {

	}

	@Override
	public CallIfc addChild(CallIfc child) {
		Preconditions.checkArgument(child instanceof FilterIfc);
		return super.addChild(child);
	}

	public List<FilterIfc> getFilters() {
		return Lists.transform(children, new Function<CallIfc, FilterIfc>() {
			
			@Override
			public FilterIfc apply(CallIfc child)
			{
				return (FilterIfc)child;
			}

		});
	}
}
