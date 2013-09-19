package cosmos.sql.call;

import com.google.common.hash.PrimitiveSink;

public class Literal extends ChildVisitor {

	private String literal;

	public Literal(String literal) {
		this.literal = literal;
	}

	public String toString() {
		return literal;
	}

	@Override
	public void funnel(ChildVisitor from, PrimitiveSink into) {
		into.putString(((Literal) from).literal);

	}

}
