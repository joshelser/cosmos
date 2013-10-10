package cosmos.sql.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import cosmos.sql.call.Field;
import cosmos.sql.call.Literal;
import cosmos.sql.call.impl.FieldEquality;
import cosmos.sql.call.impl.operators.OrOperator;

public class FilterFilterTest {

	@Test(expected = NullPointerException.class)
	public void testNull()
	{
		FilterFilter filter = new FilterFilter();
		filter.apply(null);
	}
	
	@Test
	public void testOrFalse()
	{
		FilterFilter filter = new FilterFilter();
		assertFalse(filter.apply(new OrOperator()));
	}
	

	@Test
	public void testConjunctionPositive()
	{
		FilterFilter filter = new FilterFilter();
		assertTrue(filter.apply(new FieldEquality(new Field("field"),new Literal("literal"))));
	}
}
