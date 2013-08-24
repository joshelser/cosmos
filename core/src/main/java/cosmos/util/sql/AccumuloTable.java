package cosmos.util.sql;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

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

import com.google.common.collect.Queues;

import cosmos.util.sql.AccumuloRel.Plan;

public abstract class AccumuloTable<T> extends AbstractQueryable<T> implements
		TranslatableTable<T> {

	Class<?> rawType;

	protected AccumuloSchema<? extends SchemaDefiner<?>> schema;

	protected String tableName;

	protected AccumuloIterables<T> resultSet;

	protected Queue<Plan> plans;

	public AccumuloTable(
			final AccumuloSchema<? extends SchemaDefiner<?>> schema,
			final String tableName, JavaTypeFactory typeFactory) {
		this.schema = schema;
		this.tableName = tableName;
		plans = Queues.newPriorityQueue();
		resultSet = new AccumuloIterables<T>();

	}

	@Override
	public Expression getExpression() {
		return Expressions.convert_(Expressions.call(schema.getExpression(),
				"getTable", Expressions.constant(tableName),
				Expressions.constant(getElementType())), AccumuloTable.class);

	}

	@Override
	public QueryProvider getProvider() {
		return schema.getQueryProvider();
	}

	@Override
	public Iterator<T> iterator() {
		return Linq4j.enumeratorIterator(enumerator());
	}

	@Override
	public DataContext getDataContext() {
		return schema;
	}

	@Override
	public Statistic getStatistic() {
		return Statistics.UNKNOWN;
	}

	public boolean enqueue(Plan plan) {
		boolean value = plans.add(plan);
		System.out.println(" null plan ? " + (plan == null) + " " + value);
		return value;
	}

	public abstract Enumerable<T> accumulate(List<String> fieldNameList);

	@Override
	public Enumerator<T> enumerator() {

		return Linq4j.enumerator(new ArrayList<T>());
	}

	public void execute(Plan relationalExpression) {

	}

}
