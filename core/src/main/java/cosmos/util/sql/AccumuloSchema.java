
package cosmos.util.sql;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.TableInSchemaImpl;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.impl.java.MapSchema;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeFactory.FieldInfoBuilder;

import cosmos.options.Index;

public class AccumuloSchema<T extends SchemaDefiner<?>> extends MapSchema {

	protected T meataData;

	private Class<? extends AccumuloTable<?>> clazz = null;

	/**
	 * Creates a MONGO schema.
	 * 
	 * @param parentSchema
	 *            Parent schema
	 * @param name
	 *            Name of schema
	 * @param host
	 *            Mongo host, e.g. "localhost"
	 * @param database
	 *            Mongo database name, e.g. "foodmart"
	 */
	public AccumuloSchema(Schema parentSchema, String name, String host,
			String database, Expression expression, T schemaDefiner,
			Class<? extends AccumuloTable<?>> clazz) {
		super(parentSchema, name, expression);
		meataData = schemaDefiner;
		this.clazz = clazz;

		System.out.println("table is " + meataData.getDataTable() + " name is "
				+ name);

	}

	public AccumuloTable getTable(String name) {
		System.out.println("Getting table " + name);
		return (AccumuloTable) tableMap.get(name).getTable(Class.class);
	}
	
	@Override
	public <E> Table<E> getTable(String name, Class<E> elementType) {
		System.out.println("Getting table " + name);
		return (AccumuloTable) tableMap.get(name).getTable(Class.class);
	}


	@Override
	protected Collection<TableInSchema> initialTables() {
		System.out.println("table2 is " + meataData.getDataTable());
		final List<TableInSchema> list = new ArrayList<TableInSchema>();

		FieldInfoBuilder builder = new RelDataTypeFactory.FieldInfoBuilder();

		for (Index indexField : meataData.getIndexColumns()) {
			builder.add(indexField.column().column(),
					typeFactory.createType(indexField.getIndexTyped()));
		}

		final RelDataType rowType = typeFactory.createStructType(builder);

		AccumuloTable table;
		try {
			table = table = clazz.getConstructor(AccumuloSchema.class,
					SchemaDefiner.class,JavaTypeFactory.class,
					RelDataType.class).newInstance(this, meataData,
					typeFactory, rowType);

			list.add(new TableInSchemaImpl(this, meataData.getDataTable(),
					TableType.TABLE, table));

			return list;

		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

}

// End MongoSchema.java
