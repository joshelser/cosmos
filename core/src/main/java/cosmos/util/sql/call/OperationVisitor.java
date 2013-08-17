package cosmos.util.sql.call;

import net.hydromatic.linq4j.Ord;

import org.eigenbase.rel.RelNode;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexVisitorImpl;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.SqlSyntax;
import org.eigenbase.sql.fun.SqlStdOperatorTable;

import com.google.common.base.Preconditions;

import cosmos.util.sql.call.impl.FieldEquality;

public class OperationVisitor extends RexVisitorImpl<CallIfc> {
	final StringBuilder buf = new StringBuilder();
	private final RelNode input;

	public OperationVisitor(RelNode input) {
		super(true);
		this.input = input;
	}

	@Override
	public CallIfc visitCall(RexCall call) {
		final SqlSyntax syntax = call.getOperator().getSyntax();
		switch (syntax) {
		case Binary:
			buf.append("(");

			System.out.println(syntax.getClass());
			System.out.println(call.getOperator().getClass());
			return visitBinary(call);
			// return call.getOperands().get(1).accept(this).append(")");
		case Function:
			buf.append(call.getOperator().getName().toLowerCase()).append("(");
			for (Ord<RexNode> operand : Ord.zip(call.getOperands())) {
				buf.append(operand.i > 0 ? ", " : "");
				operand.e.accept(this);
			}
			// return buf.append(")");
			return null;
		case Special:
			switch (call.getKind()) {
			case Cast:
				// Ignore casts. Drill is type-less.
				return call.getOperands().get(0).accept(this);
			}
			if (call.getOperator() == SqlStdOperatorTable.itemOp) {
				final RexNode left = call.getOperands().get(0);
				final RexLiteral literal = (RexLiteral) call.getOperands().get(
						1);
				final String field = (String) literal.getValue2();
				final int length = buf.length();
				left.accept(this);
				if (buf.length() > length) {
					// check before generating empty LHS if inputName is null
					buf.append('.');
				}
				// return buf.append(field);
				return null;
			}
			// fall through
		default:
			throw new AssertionError("todo: implement syntax " + syntax + "("
					+ call + ")");
		}
	}

	public CallIfc visitBinary(RexCall binarySyntax) {

		CallIfc left = binarySyntax.getOperands().get(0).accept(this);
		CallIfc right = binarySyntax.getOperands().get(1).accept(this);

		SqlOperator operator = binarySyntax.getOperator();

		switch (operator.getKind()) {
		case EQUALS:

			return buildEquals(left, right);

		default:
			return new Literal(operator.getName());
		}

	}

	private CallIfc buildEquals(CallIfc left, CallIfc right) {

		Preconditions.checkArgument(left instanceof Field);
		Preconditions.checkArgument(right instanceof Literal);

		return new FieldEquality((Field) left, (Literal) right);
	}

	@Override
	public String toString() {
		return buf.toString();
	}

	@Override
	public CallIfc visitInputRef(RexInputRef inputRef) {

		System.out.println("Input reaf");
		final int index = inputRef.getIndex();
		final RelDataTypeField field = input.getRowType().getFieldList()
				.get(index);
		return new Field(field.getName());
	}

	@Override
	public CallIfc visitLiteral(RexLiteral literal) {
		
		return new Literal(RexLiteral.stringValue(literal));
	}

}
