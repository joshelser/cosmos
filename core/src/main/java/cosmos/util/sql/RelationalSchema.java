package cosmos.util.sql;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.TableInSchemaImpl;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import cosmos.impl.SortableResult;
import cosmos.options.Index;
import cosmos.util.sql.impl.CosmosTable;

public class RelationalSchema extends SortableResult implements Schema {

	protected QueryProvider queryProvider;
	protected JavaTypeFactory javaFactory;
	protected Expression currentExpression;

	public Connector myConnector;
	protected AccumuloTable tableAccessor;

	protected final Map<String, TableInSchema> tables;

	public RelationalSchema(QueryProvider queryProvider,
			JavaTypeFactory typeFactory, Expression initialExpression,
			Connector connector, Authorizations auths,
			Set<Index> columnsToIndex, boolean lockOnUpdates) {
		super(connector, auths, columnsToIndex, lockOnUpdates);
		myConnector = connector;
System.out.println("fuck it");
		this.queryProvider = queryProvider;
		this.javaFactory = typeFactory;
		this.currentExpression = initialExpression;
		//tableAccessor = new ResultTable(this,typeFactory);

		tables = Maps.newHashMap();

		tables.put(dataTable, new TableInSchemaImpl(this, dataTable(),
				TableType.TABLE, tableAccessor));
	}

	@Override
	public Schema getSubSchema(String name) {
		System.out.println("writing schema " + name);
		return null;
	}

	@Override
	public JavaTypeFactory getTypeFactory() {
		return javaFactory;
	}

	@Override
	public Schema getParentSchema() {
		System.out.println(dataTable);
		return null;
	}

	@Override
	public String getName() {
		System.out.println(dataTable);
		return dataTable();
	}

	@Override
	public Collection<TableFunctionInSchema> getTableFunctions(String name) {
		System.out.println("huh?" + name);
		return Collections.emptyList();
	}

	
	@Override
	public  <E> Table<E> getTable(String name, Class<E> elementType) {
		System.out.println("huh? getTable " + name + " " + elementType);
		if (name.equals(dataTable))
		{
			System.out.println("returning");
			return  tableAccessor;
		}
		else
			return null;
	}
	
	public Table getTable()
	{
		return tableAccessor;
	}
	
	

	@Override
	public Expression getExpression() {
		System.out.println("huh? getExpression");
		return currentExpression;
	}

	@Override
	public QueryProvider getQueryProvider() {
		System.out.println("huh?");
		return queryProvider;
	}

	@Override
	public Multimap<String, TableFunctionInSchema> getTableFunctions() {
		System.out.println("huh? getTableFunctions");
		return ArrayListMultimap.create();
	}

	@Override
	public Collection<String> getSubSchemaNames() {
		System.out.println(dataTable);
		return Collections.emptyList();
	}

	@Override
	public Map<String, TableInSchema> getTables() {
		System.out.println("getting tables" + tables.keySet());
		return tables;
	}

}
