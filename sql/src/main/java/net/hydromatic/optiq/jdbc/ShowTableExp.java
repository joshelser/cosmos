package net.hydromatic.optiq.jdbc;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import net.hydromatic.linq4j.function.Function0;
import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.Schema.TableInSchema;
import net.hydromatic.optiq.jdbc.OptiqResultSet;
import net.hydromatic.optiq.jdbc.OptiqResultSetMetaData;
import net.hydromatic.optiq.jdbc.OptiqStatement;
import net.hydromatic.optiq.runtime.ColumnMetaData;
import net.hydromatic.optiq.runtime.Cursor;
import net.hydromatic.optiq.runtime.Cursor.Accessor;

import com.google.common.collect.Lists;

import cosmos.sql.rules.ResultSetRule;

public class ShowTableExp implements ResultSetRule {

  @Override
  public ResultSet execute(Statement statement, Schema parentSchema) {
    ColumnMetaData metadata = new ColumnMetaData(1, false, true, false, false, 0, false, 256, "Tables", "Tables", "Tables", 0, 0, "Tables", "", 0, "varchar",
        true, false, false, "Tables", (Class) String.class);
    List<ColumnMetaData> metadataList = Lists.newArrayList();
    metadataList.add(metadata);

    final MutableSchema schema = (MutableSchema) parentSchema;

    final OptiqStatement parent = (OptiqStatement) statement;

    final Iterator<Entry<String,TableInSchema>> blahIter = schema.getSubSchema("cosmos").getTables().entrySet().iterator();

    OptiqResultSetMetaData resultData = new OptiqResultSetMetaData(parent, "!tables", metadataList);
    OptiqResultSet newSet = new OptiqResultSet(parent, metadataList, resultData, new Function0<Cursor>() {
      public Cursor apply() {
        return new Cursor() {
          public List<Accessor> createAccessors(List<ColumnMetaData> types) {

            List<Accessor> accessor = Lists.newArrayList();

            accessor.add(new StringAccessor(blahIter));
            return accessor;
          }

          public boolean next() {

            return blahIter.hasNext();
          }

          public void close() {
            // no resources to release
          }
        };
      }
    });

    return newSet.execute();
  }

}
