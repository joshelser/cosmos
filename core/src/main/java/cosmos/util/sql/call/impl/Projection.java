package cosmos.util.sql.call.impl;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import cosmos.util.sql.call.CallIfc;
import cosmos.util.sql.call.ChildVisitor;
import cosmos.util.sql.call.Field;

public class Projection extends ChildVisitor {

	public Projection() {

	}

	@Override
	public CallIfc addChild(String id, CallIfc child) {
		Preconditions.checkArgument(child instanceof Field);
		return super.addChild(id, child);
	}

	public List<Field> getProjections() {
		return Lists.newArrayList(Iterables.transform(children.values(),
				new Function<CallIfc, Field>() {

					@Override
					public Field apply(CallIfc child) {
						return (Field) child;
					}

				}));
	}
}
