package cosmos.util.sql.call.impl;

import java.util.Collection;

import org.eigenbase.sql.SqlOperator;

import com.google.common.hash.PrimitiveSink;

import cosmos.util.sql.call.ChildVisitor;

public class Operator extends ChildVisitor {

	private SqlOperator operator;

	public Operator(SqlOperator operator) {
		this.operator = operator;
	}

	public Operator() {
		this.operator = null;
	}

	public Collection<ChildVisitor> getChildren() {
		return children.values();
	}

	@Override
	public void funnel(ChildVisitor from, PrimitiveSink into) {
		for(ChildVisitor visitor : getChildren())
		{
			visitor.funnel(visitor, into);
		}
		
	}

}
