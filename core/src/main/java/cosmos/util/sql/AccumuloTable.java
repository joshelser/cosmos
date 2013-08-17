package cosmos.util.sql;

import java.util.ArrayList;
import java.util.Iterator;

import net.hydromatic.linq4j.AbstractQueryable;
import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.Linq4j;
import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.Statistic;
import net.hydromatic.optiq.Statistics;
import net.hydromatic.optiq.TranslatableTable;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import cosmos.util.sql.AccumuloRel.Implementor;

public abstract class AccumuloTable<T> extends AbstractQueryable<T> implements
		TranslatableTable<T> {

	Class<?> rawType;
	protected AccumuloSchema<? extends SchemaDefiner> schema;
	protected String tableName;
	
	

	protected AccumuloIterables<T> resultSet;
	protected SelectQuery query;
	


	public AccumuloTable(final AccumuloSchema<? extends SchemaDefiner> schema, final String tableName, JavaTypeFactory typeFactory) {
		this.schema = schema;
		this.tableName = tableName;
		
		resultSet = new AccumuloIterables<T>();
		

	}
	
	@Override
	public Expression getExpression() {
		System.out.println("expr " + getElementType() + " " + tableName);
		
		return Expressions.convert_(
		        Expressions.call(
		            schema.getExpression(),
		            "getTable",
		            Expressions.constant(tableName),Expressions.constant(getElementType())),
		        AccumuloTable.class);
		

		
	}

	@Override
	public QueryProvider getProvider() {
		System.out.println("dsg");
		return schema.getQueryProvider();
	}

	

	@Override
	public Iterator<T> iterator() {
		System.out.println("hasd");
		return Linq4j.enumeratorIterator(enumerator());
	}

	@Override
	public DataContext getDataContext() {
		System.out.println("dsg");
		return schema;
	}

	@Override
	public Statistic getStatistic() {
		return Statistics.UNKNOWN;
	}

	
	public abstract Enumerable<T> accumulate();

	protected void select(SelectQuery query)
	{
		this.query = query;
		
		
		
		
	}
	
	@Override
	public Enumerator<T> enumerator() {
		
		return Linq4j.enumerator(new ArrayList<T>());
	}

	public void query(Implementor relationalExpression) {
		
		if (relationalExpression instanceof SelectQuery)
		{
			select((SelectQuery)relationalExpression);
		}
	}
	
	



}
