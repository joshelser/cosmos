package cosmos.util.sql.call;


public class Literal extends ChildVisitor {

	private String literal;

	public Literal(String literal) {
		this.literal = literal;
	}

	@Override
	public CallIfc addChild(CallIfc operation) {
		throw new IllegalArgumentException();
	}

	public String toString() {
		return literal;
	}



}
