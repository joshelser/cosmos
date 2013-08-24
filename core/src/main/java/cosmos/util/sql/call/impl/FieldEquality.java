package cosmos.util.sql.call.impl;

import java.util.Collection;

import cosmos.util.sql.call.ChildVisitor;
import cosmos.util.sql.call.Field;
import cosmos.util.sql.call.FilterIfc;
import cosmos.util.sql.call.Literal;
import cosmos.util.sql.call.Pair;

public class FieldEquality extends ChildVisitor<Pair<Field,Literal>> implements FilterIfc {

	
	public FieldEquality(Field left, Literal right) {
		Pair<Field,Literal> childPair = new Pair<Field,Literal>(left,right);
		addChild(childPair.getClass().getSimpleName(),childPair);
		
		

	}
	
	public Collection<Pair<Field,Literal>> getChildren() 
	{
		return children.get(Pair.class.getSimpleName());
	}
	
	

}
