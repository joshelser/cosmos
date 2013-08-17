package cosmos.util.sql.call.impl;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import cosmos.util.sql.call.CallIfc;
import cosmos.util.sql.call.ChildVisitor;
import cosmos.util.sql.call.Field;

public class Projection extends ChildVisitor {

	public Projection() {

	}

	@Override
	public CallIfc addChild(CallIfc child) {
		Preconditions.checkArgument(child instanceof Field);
		return super.addChild(child);
	}

	public List<Field> getProjections() {
		return Lists.transform(children, new Function<CallIfc, Field>() {
			
			@Override
			public Field apply(CallIfc child)
			{
				return (Field)child;
			}

		});
	}
}
