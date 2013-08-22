
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

import org.apache.log4j.Logger;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeFactory.FieldInfoBuilder;

import cosmos.options.Index;

public class AccumuloSchema<T extends SchemaDefiner<?>> extends MapSchema {

	protected T meataData;

	private Class<? extends AccumuloTable<?>> clazz = null;

	private static final Logger log = Logger.getLogger(AccumuloSchema.class);
	/**
	 * Accumulo schema constructor
	 */
	public AccumuloSchema(Schema parentSchema, String name, String host,
			String database, Expression expression, T schemaDefiner,
			Class<? extends AccumuloTable<?>> clazz) {
		super(parentSchema, name, expression);
		meataData = schemaDefiner;
		this.clazz = clazz;
	}

	/**
	 * Returns the table associated with the class
	 */
	public AccumuloTable<?> getTable(String name) {

		return (AccumuloTable<?>) tableMap.get(name).getTable(Class.class);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <E> Table<E> getTable(String name, Class<E> elementType) {
		return  (Table<E>) tableMap.get(name).getTable(Class.class);
	}


	@Override
	protected Collection<TableInSchema> initialTables() {
		final List<TableInSchema> list = new ArrayList<TableInSchema>();

		FieldInfoBuilder builder = new RelDataTypeFactory.FieldInfoBuilder();

		for (Index indexField : meataData.getIndexColumns()) {
			builder.add(indexField.column().column(),
					typeFactory.createType(indexField.getIndexTyped()));
		}

		final RelDataType rowType = typeFactory.createStructType(builder);

		AccumuloTable<?> table;
		try {
			table =  clazz.getConstructor(AccumuloSchema.class,
					SchemaDefiner.class,JavaTypeFactory.class,
					RelDataType.class).newInstance(this, meataData,
					typeFactory, rowType);

			list.add(new TableInSchemaImpl(this, meataData.getDataTable(),
					TableType.TABLE, table));

			return list;

		} catch (InstantiationException e) {
			log.error(e);
		} catch (IllegalAccessException e) {
			log.error(e);
		} catch (IllegalArgumentException e) {
			log.error(e);
		} catch (InvocationTargetException e) {
			log.error(e);
		} catch (NoSuchMethodException e) {
			log.error(e);
		} catch (SecurityException e) {
			log.error(e);
		}
		return null;

	}

}

