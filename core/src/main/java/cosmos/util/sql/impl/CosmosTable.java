package cosmos.util.sql.impl;

import java.lang.reflect.Type;
import java.util.Map.Entry;

import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Linq4j;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptTable.ToRelContext;
import org.eigenbase.reltype.RelDataType;

import cosmos.results.impl.MultimapQueryResult;
import cosmos.util.sql.AccumuloRel;
import cosmos.util.sql.AccumuloSchema;
import cosmos.util.sql.AccumuloTable;
import cosmos.util.sql.SchemaDefiner;
import cosmos.util.sql.TableScanner;


/**
 * Cosmos table representation.
 * @author phrocker
 *
 */
public class CosmosTable extends AccumuloTable<MultimapQueryResult> {

	protected JavaTypeFactory javaFactory;

	protected RelDataType rowType;

	private SchemaDefiner metadata;

	public CosmosTable(AccumuloSchema<? extends SchemaDefiner> meataSchema,
			SchemaDefiner metadata, JavaTypeFactory typeFactory,
			RelDataType rowType) {

		super(meataSchema, metadata.getDataTable(), typeFactory);
		javaFactory = typeFactory;

		this.metadata =  metadata;

		this.rowType = rowType;

	}


	@Override
	public RelDataType getRowType() {

		return rowType;
	}

	@Override
	public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
		return new TableScanner(context.getCluster(), context
				.getCluster().traitSetOf(AccumuloRel.CONVENTION), relOptTable,
				this, relOptTable.getRowType()
						.getFieldNames());
	}

	public Enumerable<MultimapQueryResult> accumulate()
	{
		resultSet = metadata.iterator(query);
		return Linq4j.asEnumerable(resultSet);
	}


	@Override
	public Type getElementType() {
		return MultimapQueryResult.class;
	}
	
	
	

}
