package cosmos.util.sql;

import net.hydromatic.optiq.impl.java.JavaTypeFactory;

public abstract class ResultTable extends AccumuloTable<Object[]> {

	public ResultTable(AccumuloSchema<? extends SchemaDefiner> schema,
			String tableName, JavaTypeFactory typeFactory) {
		super(schema, tableName, typeFactory);
	}

}
