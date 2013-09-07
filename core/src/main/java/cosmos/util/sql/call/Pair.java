package cosmos.util.sql.call;

import com.google.common.hash.PrimitiveSink;


public class Pair<T extends ChildVisitor,K extends ChildVisitor> extends ChildVisitor {
	
	ChildVisitor left = null;
	ChildVisitor right = null;
	public Pair(T left, K right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public CallIfc<?> addChild(String id, ChildVisitor child) {
		super.addChild(id, child);
		return this;
	}
	
	public CallIfc<?> first() 
	{
		return left;
	}
	
	public CallIfc<?> second() 
	{
		return right;
	}

	@Override
	public void funnel(ChildVisitor from, PrimitiveSink into) {
		Pair<ChildVisitor,ChildVisitor> child = (Pair<ChildVisitor, ChildVisitor>) from;
		child.left.funnel(child.left, into);
		child.right.funnel(child.right, into);
		
		
	}

}
