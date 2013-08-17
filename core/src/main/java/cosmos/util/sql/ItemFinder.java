package cosmos.util.sql;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexShuttle;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.SqlTypeName;

public class ItemFinder extends RexShuttle {
	    public final Map<String, RexInputRef> map =
	        new LinkedHashMap<String, RexInputRef>();
	    private final RelDataTypeFactory.FieldInfoBuilder builder =
	        new RelDataTypeFactory.FieldInfoBuilder();
	    public List<String> items = new ArrayList<String>();

	    @Override
	    public RexNode visitCall(RexCall call) {
	      String fieldName = parseFieldAccess(call);
	      System.out.println("fieldName " + fieldName);
	      if (fieldName != null) {
	        return registerField(fieldName, call.getType());
	      }
	      RelDataType type = parseCast(call);
	      if (type != null) {
	        final RexNode operand = call.getOperands().get(0);
	        fieldName = parseFieldAccess(operand);
	        if (fieldName != null) {
	          return registerField(fieldName, call.getType());
	        }
	        // just ignore the cast
	        return operand.accept(this);
	      }
	      return super.visitCall(call);
	    }

	    private RexNode registerField(String fieldName, RelDataType type) {
	      RexInputRef x = map.get(fieldName);
	      if (x == null) {
	        x = new RexInputRef(map.size(), type);
	        map.put(fieldName, x);
	        builder.add(fieldName, type);
	        items.add(fieldName + ": 1");
	      }
	      return x;
	    }
	    

		  private static String parseFieldAccess(RexNode rex) {
		    if (rex instanceof RexCall) {
		      final RexCall call = (RexCall) rex;
		      if (call.getOperator() == SqlStdOperatorTable.itemOp
		          && call.getOperands().size() == 2
		          && call.getOperands().get(0) instanceof RexInputRef
		          && ((RexInputRef) call.getOperands().get(0)).getIndex() == 0
		          && call.getOperands().get(1) instanceof RexLiteral) {
		        RexLiteral arg = (RexLiteral) call.getOperands().get(1);
		        if (arg.getTypeName() == SqlTypeName.CHAR) {
		          return (String) arg.getValue2();
		        }
		      }
		    }
		    return null;
		  }

		  private static RelDataType parseCast(RexNode rex) {
		    if (rex instanceof RexCall) {
		      final RexCall call = (RexCall) rex;
		      if (call.getOperator() == SqlStdOperatorTable.castFunc) {
		        assert call.getOperands().size() == 1;
		        return call.getType();
		      }
		    }
		    return null;
		  }
		
	  }

