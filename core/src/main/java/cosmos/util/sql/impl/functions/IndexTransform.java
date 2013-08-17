package cosmos.util.sql.impl.functions;

import java.util.Map.Entry;

import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactoryImpl;
import org.eigenbase.sql.type.SqlTypeName;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import cosmos.options.Index;

/**
 * Transforms indexes into an immutable entry describing the indexed entry
 * @author marc
 *
 */
public class IndexTransform implements
		Function<Index, Entry<String, RelDataType>> {

	
	protected JavaTypeFactory javaFactory;

	public IndexTransform(JavaTypeFactory javaFactory) {
		this.javaFactory = javaFactory;
	}

	@Override
	public Entry<String, RelDataType> apply(Index input) {
		
		return Maps.immutableEntry(input.column().toString(), javaFactory.createSqlType(SqlTypeName.VARCHAR));
	}

}
