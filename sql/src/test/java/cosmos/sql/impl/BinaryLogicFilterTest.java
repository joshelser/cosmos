package cosmos.sql.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import cosmos.sql.call.impl.operators.AndOperator;
import cosmos.sql.call.impl.operators.OrOperator;

public class BinaryLogicFilterTest {

	@Test(expected = NullPointerException.class)
	public void testNull()
	{
		BinaryLogicFilter filter = new BinaryLogicFilter();
		filter.apply(null);
	}
	
	@Test
	public void testOrFalse()
	{
		BinaryLogicFilter filter = new BinaryLogicFilter();
		assertFalse(filter.apply(new OrOperator()));
	}
	

	@Test
	public void testConjunctionPositive()
	{
		BinaryLogicFilter filter = new BinaryLogicFilter();
		assertTrue(filter.apply(new AndOperator()));
	}
}
