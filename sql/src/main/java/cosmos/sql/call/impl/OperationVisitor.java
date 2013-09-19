package cosmos.sql.call.impl;

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

import cosmos.sql.call.ChildVisitor;
import cosmos.sql.call.Field;
import cosmos.sql.call.Literal;
import cosmos.sql.call.impl.operators.AndOperator;
import cosmos.sql.call.impl.operators.OrOperator;

public class OperationVisitor extends RexVisitorImpl<ChildVisitor> {
	final StringBuilder buf = new StringBuilder();
	private final RelNode input;

	public OperationVisitor(RelNode input) {
		super(true);
		this.input = input;
	}

	@Override
	public ChildVisitor visitCall(RexCall call) {
		final SqlSyntax syntax = call.getOperator().getSyntax();
		switch (syntax) {
		case Binary:
			return visitBinary(call);
		case Function:
			buf.append(call.getOperator().getName().toLowerCase()).append("(");
			for (Ord<RexNode> operand : Ord.zip(call.getOperands())) {
				buf.append(operand.i > 0 ? ", " : "");
				operand.e.accept(this);
			}
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

	public ChildVisitor visitBinary(RexCall binarySyntax) {

		ChildVisitor left = binarySyntax.getOperands().get(0).accept(this);
		ChildVisitor right = binarySyntax.getOperands().get(1).accept(this);

		SqlOperator operator = binarySyntax.getOperator();
		Operator op = null;
		switch (operator.getKind()) {
		case EQUALS:
			op = new FieldEquality(left, right);
			break;
		case AND:
			op = new AndOperator();
			break;
		case OR:
			op = new OrOperator();
			break;
		default:
			op = new Operator(operator);

		}
		op.addChild("left", left);
		op.addChild("right", right);
		return op;

	}

	@Override
	public String toString() {
		return buf.toString();
	}

	@Override
	public ChildVisitor visitInputRef(RexInputRef inputRef) {

		final int index = inputRef.getIndex();
		final RelDataTypeField field = input.getRowType().getFieldList()
				.get(index);
		return new Field(field.getName());
	}

	@Override
	public ChildVisitor visitLiteral(RexLiteral literal) {

		return new Literal(RexLiteral.stringValue(literal));
	}

}
