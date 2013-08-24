package cosmos.util.sql.call;


public class Pair<T extends CallIfc<?>,K extends CallIfc<?>> extends ChildVisitor {
	
	CallIfc<?> left = null;
	CallIfc<?> right = null;
	public Pair(T left, K right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public CallIfc<?> addChild(String id, CallIfc child) {
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

}
