package cosmos.util.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.jdbc.DriverVersion;
import net.hydromatic.optiq.jdbc.OptiqConnection;
import net.hydromatic.optiq.jdbc.UnregisteredDriver;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.dbcp.BasicDataSource;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;

import cosmos.UnexpectedStateException;
import cosmos.impl.CosmosImpl;
import cosmos.impl.SortableResult;
import cosmos.mediawiki.MediawikiPage.Page;
import cosmos.mediawiki.MediawikiPage.Page.Revision;
import cosmos.mediawiki.MediawikiPage.Page.Revision.Contributor;
import cosmos.options.Index;
import cosmos.results.Column;
import cosmos.results.SValue;
import cosmos.results.impl.MultimapQueryResult;
import cosmos.util.sql.impl.CosmosSql;
import cosmos.util.sql.impl.CosmosTable;

/**
 * JDBC Driver.
 */
public class AccumuloDriver extends UnregisteredDriver {

	private static final String ACCUMULO = "accumulo";
	protected SchemaDefiner<?> definer;
	private AccumuloSchema<CosmosSql> schema;

	protected String jdbcConnector = null;

	protected AccumuloDriver(String connectorName) {
		super();
		jdbcConnector = connectorName;
	}

	public AccumuloDriver(SchemaDefiner<?> definer) {
		this(ACCUMULO);
		this.definer = definer;
		register();
	}

	protected String getConnectStringPrefix() {
		return "jdbc:accumulo:";
	}

	protected DriverVersion createDriverVersion() {
		return new AccumuloJdbcDriverVersion();
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		Connection connection = super.connect(url, info);
		OptiqConnection optiqConnection = (OptiqConnection) connection;

		final MutableSchema rootSchema = optiqConnection.getRootSchema();
		final String schemaName = "sorts";

		// optiqConnection.setSchema("");
		try {
			schema = new AccumuloSchema<CosmosSql>(rootSchema, "sorts",
					"sorts", "sorts", rootSchema.getSubSchemaExpression(
							schemaName, AccumuloSchema.class),
					(CosmosSql) definer, CosmosTable.class);

			schema.initialize();
			rootSchema.addSchema(schemaName, schema);

			Map<String, Object> users = Maps.newHashMap();
			users.put("admin", "changeme");

			BasicDataSource dataSource = new BasicDataSource();
			dataSource.setUrl(getConnectStringPrefix() + "//localhost");
			dataSource.setUsername("admin");
			dataSource.setPassword("changeme");

			// JdbcSchema.create(rootSchema,dataSource,"admin","",schemaName);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return optiqConnection;
	}

}

// End SplunkDriver.java
