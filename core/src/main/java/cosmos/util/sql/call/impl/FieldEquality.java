package cosmos.util.sql.call.impl;

import cosmos.util.sql.call.BinaryCaller;
import cosmos.util.sql.call.Field;
import cosmos.util.sql.call.FilterIfc;
import cosmos.util.sql.call.Literal;

public class FieldEquality extends BinaryCaller implements FilterIfc {

	public FieldEquality(Field left, Literal right) {
		super(left, right);

	}
	
	public Field getField()
	{
		return (Field) children.get(0);
	}
	
	
	public Literal getLiteral()
	{
		return (Literal) children.get(1);
	}
	
	

}
