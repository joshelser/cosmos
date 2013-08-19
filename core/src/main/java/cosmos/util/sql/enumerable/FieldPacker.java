package cosmos.util.sql.enumerable;

import net.hydromatic.optiq.rules.java.EnumerableConvention;

import org.eigenbase.rel.CalcRel;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.Convention;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexMultisetUtil;
import org.eigenbase.rex.RexProgram;

import cosmos.util.sql.TableScanner;

/**
 * Creates a reference to the table scanner and provides it
 * the list of fields that the user wants while we're parsing the 
 * SQL expression
 * @author phrocker
 *
 */
public class FieldPacker extends ConverterRule {
	private TableScanner scanner;

	public FieldPacker(TableScanner scanner) {
		super(CalcRel.class, Convention.NONE,
				EnumerableConvention.INSTANCE, "FieldPacker");
		this.scanner = scanner;
	}

	public RelNode convert(RelNode rel) {
		final CalcRel calc = (CalcRel) rel;
		final RexProgram program = calc.getProgram();
		
		RelDataType rowType = program.getOutputRowType();
		
		scanner.setFields(rowType.getFieldNames());
		
		if (RexMultisetUtil.containsMultiset(program) || program.containsAggs()) {
			return null;
		}

		return new FieldPackerRelation(rel.getCluster(), rel.getTraitSet()
				.replace(EnumerableConvention.INSTANCE), convert(
				calc.getChild(),
				calc.getChild().getTraitSet()
						.replace(EnumerableConvention.INSTANCE)), program,
				ProjectRelBase.Flags.Boxed);
	}
}