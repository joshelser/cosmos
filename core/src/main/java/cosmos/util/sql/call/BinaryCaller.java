package cosmos.util.sql.call;

public class BinaryCaller extends ChildVisitor {
	protected BinaryCaller(CallIfc left, CallIfc right) {
		addChild(left);
		addChild(right);
	}

	@Override
	public CallIfc addChild(CallIfc child) {
		super.addChild(child);
		return this;
	}
}
