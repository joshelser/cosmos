package cosmos.util.sql.call;


public class Literal extends ChildVisitor<Literal> {

	private String literal;

	public Literal(String literal) {
		this.literal = literal;
	}

	@Override
	public CallIfc addChild(String id, Literal operation) {
		throw new IllegalArgumentException();
	}

	public String toString() {
		return literal;
	}



}
