package cosmos.util.sql.impl;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.Linq4j;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptTable.ToRelContext;
import org.eigenbase.reltype.RelDataType;

import com.google.common.collect.Lists;

import cosmos.Cosmos;
import cosmos.impl.SortableResult;
import cosmos.options.Index;
import cosmos.util.sql.AccumuloIterables;
import cosmos.util.sql.AccumuloRel;
import cosmos.util.sql.AccumuloSchema;
import cosmos.util.sql.AccumuloTable;
import cosmos.util.sql.TableScanner;
import cosmos.util.sql.SchemaDefiner;
import cosmos.util.sql.call.Field;

public class CosmosTable extends AccumuloTable<Entry<Key, Value>> {

	protected JavaTypeFactory javaFactory;

	protected RelDataType rowType;

	private SchemaDefiner metadata;

	public CosmosTable(AccumuloSchema<? extends SchemaDefiner> meataSchema,
			SchemaDefiner metadata, JavaTypeFactory typeFactory,
			RelDataType rowType) {

		super(meataSchema, metadata.getDataTable(), typeFactory);
		System.out.println("new table");
		javaFactory = typeFactory;

		this.metadata =  metadata;

		this.rowType = rowType;
		initialize();
	}

	protected void initialize() {

		Set<Index> indexedColumns = metadata.getIndexColumns();

		for (Index index : indexedColumns) {
			System.out.println(index.column().toString());
		}


	}

	@Override
	public RelDataType getRowType() {
		System.out.println("ha");
		return rowType;
	}

	@Override
	public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
		System.out.println("ha sads s " + relOptTable.getRowType().getFieldNames());

		return new TableScanner(context.getCluster(), context
				.getCluster().traitSetOf(AccumuloRel.CONVENTION), relOptTable,
				this, relOptTable.getRowType()
						.getFieldNames());
	}

	@Override
	public Class getElementType() {
		return Entry.class;
	}

	public Enumerable<Entry<Key,Value>> accumulate()
	{
		resultSet = metadata.iterator(query);
		return Linq4j.asEnumerable(resultSet);
	}
	
	
	

}
