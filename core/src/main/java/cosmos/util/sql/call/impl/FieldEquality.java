package cosmos.util.sql.call.impl;

import java.util.Collection;

import com.google.common.base.Preconditions;

import cosmos.util.sql.call.CallIfc;
import cosmos.util.sql.call.ChildVisitor;
import cosmos.util.sql.call.Field;
import cosmos.util.sql.call.Literal;
import cosmos.util.sql.call.Pair;

public class FieldEquality extends Operator {

	public FieldEquality(CallIfc left, CallIfc right) {
		Preconditions.checkArgument(left instanceof Field);
		Preconditions.checkArgument(right instanceof Literal);
		addChild((Field) left, (Literal) right);

	}

	public FieldEquality(Field left, Literal right) {
		addChild(left, right);
	}

	private void addChild(Field left, Literal right) {
		Pair<Field, Literal> childPair = new Pair<Field, Literal>(left, right);
		addChild(childPair.getClass().getSimpleName(), childPair);
	}

	public Collection<ChildVisitor> getChildren() {
		return children.get(Pair.class.getSimpleName());
	}

}
