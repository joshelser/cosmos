package cosmos.sql.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import cosmos.records.impl.MultimapRecord;
import cosmos.records.values.RecordValue;
import cosmos.results.Column;
import cosmos.sql.call.Field;
import cosmos.sql.call.Literal;

public class DocumentFieldPredicateTest {

	
	@Test
	public void testEmptyDocument()
	{
		Multimap<Column, RecordValue<?>> values = ArrayListMultimap.create();
		MultimapRecord doc = new MultimapRecord(values,"abc",new ColumnVisibility("viz1"));
		DocumentFieldPredicate filter = new DocumentFieldPredicate(new Field("field1"),new Literal("Value1"));
		assertFalse( filter.apply(doc) );
	}	
	
	@Test
	public void testFieldButNoValue()
	{
		Multimap<Column, RecordValue<?>> values = ArrayListMultimap.create();
		values.put(new Column("field1"), RecordValue.create("Value2", new ColumnVisibility("viz1")));
		MultimapRecord doc = new MultimapRecord(values,"abc",new ColumnVisibility("viz1"));
		DocumentFieldPredicate filter = new DocumentFieldPredicate(new Field("field1"),new Literal("Value1"));
		assertFalse( filter.apply(doc) );
	}
	
	@Test
	public void testFieldWithValue()
	{
		Multimap<Column, RecordValue<?>> values = ArrayListMultimap.create();
		values.put(new Column("field1"), RecordValue.create("Value1", new ColumnVisibility("viz1")));
		MultimapRecord doc = new MultimapRecord(values,"abc",new ColumnVisibility("viz1"));
		DocumentFieldPredicate filter = new DocumentFieldPredicate(new Field("field1"),new Literal("Value1"));
		assertTrue( filter.apply(doc) );
	}	
}
