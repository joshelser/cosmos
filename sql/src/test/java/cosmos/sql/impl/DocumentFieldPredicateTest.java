package cosmos.sql.impl;

import static org.junit.Assert.*;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import cosmos.results.Column;
import cosmos.results.SValue;
import cosmos.results.impl.MultimapQueryResult;
import cosmos.sql.call.Field;
import cosmos.sql.call.Literal;

public class DocumentFieldPredicateTest {

	
	@Test
	public void testEmptyDocument()
	{
		Multimap<Column, SValue> values = ArrayListMultimap.create();
		MultimapQueryResult doc = new MultimapQueryResult(values,"abc",new ColumnVisibility("viz1"));
		DocumentFieldPredicate filter = new DocumentFieldPredicate(new Field("field1"),new Literal("Value1"));
		assertFalse( filter.apply(doc) );
	}	
	
	@Test
	public void testFieldButNoValue()
	{
		Multimap<Column, SValue> values = ArrayListMultimap.create();
		values.put(new Column("field1"), new SValue("Value2", new ColumnVisibility("viz1")));
		MultimapQueryResult doc = new MultimapQueryResult(values,"abc",new ColumnVisibility("viz1"));
		DocumentFieldPredicate filter = new DocumentFieldPredicate(new Field("field1"),new Literal("Value1"));
		assertFalse( filter.apply(doc) );
	}
	
	@Test
	public void testFieldWithValue()
	{
		Multimap<Column, SValue> values = ArrayListMultimap.create();
		values.put(new Column("field1"), new SValue("Value1", new ColumnVisibility("viz1")));
		MultimapQueryResult doc = new MultimapQueryResult(values,"abc",new ColumnVisibility("viz1"));
		DocumentFieldPredicate filter = new DocumentFieldPredicate(new Field("field1"),new Literal("Value1"));
		assertTrue( filter.apply(doc) );
	}	
}
