import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.protobuf.InvalidProtocolBufferException;

import cosmos.IntegrationTests;
import cosmos.impl.CosmosImpl;
import cosmos.impl.SortableResult;
import cosmos.mediawiki.MediawikiPage.Page;
import cosmos.mediawiki.MediawikiPage.Page.Revision;
import cosmos.mediawiki.MediawikiPage.Page.Revision.Contributor;
import cosmos.options.Index;
import cosmos.results.Column;
import cosmos.results.SValue;
import cosmos.results.impl.MultimapQueryResult;
import cosmos.util.sql.AccumuloDriver;
import cosmos.util.sql.impl.CosmosSql;

@Category(IntegrationTests.class)
public class TestSql {

	protected static File tmp = Files.createTempDir();
	protected static MiniAccumuloCluster mac;
	protected static MiniAccumuloConfig macConfig;
	private static ZooKeeperInstance instance;
	private SortableResult meataData;
	private static CosmosSql cosmosSql;

	public static final ColumnVisibility cv = new ColumnVisibility("en");
	final static Random offsetR = new Random();
	static final Random cardinalityR = new Random();

	private static int recordsReturned;
	private static Connector connector;

	public static final Column PAGE_ID = Column.create("PAGE_ID"),
			REVISION_ID = Column.create("REVISION_ID"),
			REVISION_TIMESTAMP = Column.create("REVISION_TIMESTAMP"),
			CONTRIBUTOR_USERNAME = Column.create("CONTRIBUTOR_USERNAME"),
			CONTRIBUTOR_ID = Column.create("CONTRIBUTOR_ID");

	public static final int MAX_SIZE = 16000;
	// MAX_OFFSET is a little misleading because the max pageID is 33928886
	// Don't have contiguous pageIDs
	public static final int MAX_OFFSET = 11845576 - MAX_SIZE;

	public static final int MAX_ROW = 999999999;
	
	private static Set<Index> columns = null;
	
	private static CosmosImpl impl;

	@BeforeClass
	public static void setup() throws Exception {
		macConfig = new MiniAccumuloConfig(tmp, "root");

		mac = new MiniAccumuloCluster(macConfig);

		mac.start();

		// Do this now so we don't forget later (or get an exception)
		tmp.deleteOnExit();

		instance = new ZooKeeperInstance(mac.getInstanceName(),
				mac.getZooKeepers());

		connector = instance.getConnector("root",
				ByteBuffer.wrap(macConfig.getRootPassword().getBytes()));

		connector.securityOperations().changeUserAuthorizations("root",
				new Authorizations("en"));



		

		int offset = offsetR.nextInt(MAX_OFFSET);
	

		columns = Sets.newHashSet();
		columns.add(new Index(PAGE_ID));
		columns.add(new Index(REVISION_ID));
		columns.add(new Index(REVISION_TIMESTAMP));
		columns.add(new Index(CONTRIBUTOR_ID));

		
		impl = new CosmosImpl(mac.getZooKeepers());

		cosmosSql = new CosmosSql(impl);


		

		new AccumuloDriver(cosmosSql, "cosmos");

	}

	@AfterClass
	public static void teardown() throws IOException, InterruptedException {
		mac.stop();
	}

	@Before
	public void setupVariables() throws Exception {
		
		if (!connector.tableOperations().exists("sorts")) {
		//	connector.tableOperations().delete("sorts");
			connector.tableOperations().create("sorts");
			
		}
		
		if (connector.tableOperations().exists("sortswiki")) {
			connector.tableOperations().delete("sortswiki");
		}

		
		connector.tableOperations().create("sortswiki");
		
		BatchWriterConfig bwConfig = new BatchWriterConfig();
		bwConfig.setMaxLatency(1000L, TimeUnit.MILLISECONDS);
		bwConfig.setMaxMemory(1024L);
		bwConfig.setMaxWriteThreads(1);
		BatchWriter writer = connector.createBatchWriter("sortswiki", bwConfig);

		Collection<Page> pages = buildPages(10);
		for (Page page : pages) {
			Mutation m = new Mutation(UUID.randomUUID().toString());
			m.put("cf", "cq", new Value(page.toByteArray()));
			writer.addMutation(m);
		}

		writer.close();
		
		
		int numRecords = cardinalityR.nextInt(MAX_SIZE);

		BatchScanner bs = connector.createBatchScanner("sortswiki",
				new Authorizations(), 4);

		bs.setRanges(Collections.singleton(new Range()));
		
		meataData = new SortableResult(connector, connector
				.securityOperations().getUserAuthorizations("root"), columns,
				false);

		Function<Entry<Key, Value>, MultimapQueryResult> func = new Function<Entry<Key, Value>, MultimapQueryResult>() {
			@Override
			public MultimapQueryResult apply(Entry<Key, Value> input) {
				Page p;
				try {
					p = Page.parseFrom(input.getValue().get());
				} catch (InvalidProtocolBufferException e) {
					throw new RuntimeException(e);
				}
				return pagesToQueryResult(p);
			}
		};

		Map<Column, Long> counts = Maps.newHashMap();
		ArrayList<MultimapQueryResult> tformSource = Lists
				.newArrayListWithCapacity(20000);
		Iterable<Entry<Key, Value>> inputIterable = Iterables.limit(bs,
				numRecords);
		for (Entry<Key, Value> input : inputIterable) {

			MultimapQueryResult r = func.apply(input);
			tformSource.add(r);

			loadCountsForRecord(counts, r);
			recordsReturned++;
		}
		
		

	
		impl.register(meataData);
		

		impl.addResults(meataData, tformSource);
	}

	protected static Collection<Page> buildPages(int pageCount)

	{
		Collection<Page> pages = Lists.newArrayList();
		for (int i = 0; i < pageCount; i++) {
			Page.Builder builder = Page.newBuilder();

			builder.setId(i);

			Revision.Builder revBuilder = Revision.newBuilder();
			revBuilder.setId(i);

			Contributor.Builder contribBuilder = Contributor.newBuilder();
			contribBuilder.setUsername("marcy");
			contribBuilder.setId(i + 1);
			revBuilder.setTimestamp(Long.valueOf(System.currentTimeMillis())
					.toString());
			revBuilder.setContributor(contribBuilder.build());

			builder.setRevision(revBuilder.build());

			pages.add(builder.build());

		}
		return pages;
	}

	public static final String JDBC_URL = "https://localhost:8089";
	public static final String USER = "admin";
	public static final String PASSWORD = "changeme";

	private void loadDriverClass() {
		try {
			Class.forName(AccumuloDriver.class.getCanonicalName());
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("driver not found", e);
		}
	}

	private void close(Connection connection, Statement statement) {
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException e) {
				// ignore
			}
		}
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				// ignore
			}
		}
	}

	@Test
	public void test() {
		// replace test until I can stop this test from relying on my local
		// store
	}

	@Test
	public void testDisjunctionPositive() throws SQLException {
		loadDriverClass();
		Connection connection = null;
		Statement statement = null;
		try {
			Properties info = new Properties();
			info.put("url", JDBC_URL);
			info.put("user", USER);
			info.put("password", PASSWORD);
			connection = DriverManager.getConnection(
					"jdbc:accumulo:cosmos//localhost", info);
			statement = connection.createStatement();
			final ResultSet resultSet = statement
					.executeQuery("select \"PAGE_ID\" from \"sorts\".\""
							+ meataData.uuid() + "\"  where PAGE_ID='9' or REVISION_ID='8'");
			final ResultSetMetaData metaData = resultSet.getMetaData(); 
			final int columnCount = metaData.getColumnCount();

			assertEquals(columnCount, 1);

			int resultsFound = 0;
			while (resultSet.next()) {
				assertEquals(metaData.getColumnName(1), "PAGE_ID");
				List<Entry<Column, SValue>> sValues = (List<Entry<Column, SValue>>) resultSet
						.getObject("PAGE_ID");
				assertEquals(sValues.size(), 1);
				SValue onlyValue = sValues.iterator().next().getValue();
				assertEquals(onlyValue.visibility().toString(), "[en]");
				
				assertTrue(onlyValue.value().equals(Integer.valueOf(9)
						.toString()) || onlyValue.value().equals(Integer.valueOf(8)
								.toString()));
				resultsFound++;

			}

			assertEquals(resultsFound, 2);
		} finally {
			close(connection, statement);
		}
	}
	
	@Test
	public void testConjunction() throws SQLException {
		loadDriverClass();
		Connection connection = null;
		Statement statement = null;
		try {
			Properties info = new Properties();
			info.put("url", JDBC_URL);
			info.put("user", USER);
			info.put("password", PASSWORD);
			connection = DriverManager.getConnection(
					"jdbc:accumulo:cosmos//localhost", info);
			statement = connection.createStatement();
			final ResultSet resultSet = statement
					.executeQuery("select \"PAGE_ID\" from \"sorts\".\""
							+ meataData.uuid() + "\"  where PAGE_ID='9' and REVISION_ID='9'");
			final ResultSetMetaData metaData = resultSet.getMetaData(); 
			final int columnCount = metaData.getColumnCount();

			assertEquals(columnCount, 1);

			int resultsFound = 0;
			while (resultSet.next()) {
				assertEquals(metaData.getColumnName(1), "PAGE_ID");
				List<Entry<Column, SValue>> sValues = (List<Entry<Column, SValue>>) resultSet
						.getObject("PAGE_ID");
				assertEquals(sValues.size(), 1);
				SValue onlyValue = sValues.iterator().next().getValue();
				assertEquals(onlyValue.visibility().toString(), "[en]");
				
				assertEquals(onlyValue.value(), Integer.valueOf(9)
						.toString());
				resultsFound++;
				break;

			}

			assertEquals(resultsFound, 1);
		} finally {
			close(connection, statement);
		}
	}
	
	
	@Test
	public void testLimit() throws SQLException {
		loadDriverClass();
		Connection connection = null;
		Statement statement = null;
		try {
			Properties info = new Properties();
			info.put("url", JDBC_URL);
			info.put("user", USER);
			info.put("password", PASSWORD);
			connection = DriverManager.getConnection(
					"jdbc:accumulo:cosmos//localhost", info);
			statement = connection.createStatement();
			final ResultSet resultSet = statement
					.executeQuery("select \"PAGE_ID\" from \"sorts\".\""
							+ meataData.uuid() + "\"  limit 2 OFFSET 0");
			final ResultSetMetaData metaData = resultSet.getMetaData(); 
			final int columnCount = metaData.getColumnCount();

			assertEquals(columnCount, 1);

			int resultsFound = 0;
			while (resultSet.next()) {
				assertEquals(metaData.getColumnName(1), "PAGE_ID");
				List<Entry<Column, SValue>> sValues = (List<Entry<Column, SValue>>) resultSet
						.getObject("PAGE_ID");
				assertEquals(sValues.size(), 1);
				SValue onlyValue = sValues.iterator().next().getValue();
				assertEquals(onlyValue.visibility().toString(), "[en]");
				
				assertEquals(onlyValue.value(), Integer.valueOf(resultsFound)
						.toString());
				resultsFound++;

			}

			assertEquals(resultsFound, 2);
		} finally {
			close(connection, statement);
		}
	}
	
	@Test
	public void testJoin() throws SQLException {
		loadDriverClass();
		Connection connection = null;
		Statement statement = null;
		try {
			Properties info = new Properties();
			info.put("url", JDBC_URL);
			info.put("user", USER);
			info.put("password", PASSWORD);
			connection = DriverManager.getConnection(
					"jdbc:accumulo:cosmos//localhost", info);
			statement = connection.createStatement();
			final ResultSet resultSet = statement
					.executeQuery("select \"PAGE_ID\" from \"sorts\".\""
							+ meataData.uuid() + "\"  limit 2 OFFSET 0");
			final ResultSetMetaData metaData = resultSet.getMetaData(); 
			final int columnCount = metaData.getColumnCount();

			assertEquals(columnCount, 1);

			int resultsFound = 0;
			while (resultSet.next()) {
				assertEquals(metaData.getColumnName(1), "PAGE_ID");
				List<Entry<Column, SValue>> sValues = (List<Entry<Column, SValue>>) resultSet
						.getObject("PAGE_ID");
				assertEquals(sValues.size(), 1);
				SValue onlyValue = sValues.iterator().next().getValue();
				assertEquals(onlyValue.visibility().toString(), "[en]");
				
				assertEquals(onlyValue.value(), Integer.valueOf(resultsFound)
						.toString());
				resultsFound++;

			}

			assertEquals(resultsFound, 2);
		} finally {
			close(connection, statement);
		}
	}
	
	

	@Test
	public void testNoLimit() throws SQLException {
		loadDriverClass();
		Connection connection = null;
		Statement statement = null;
		try {
			Properties info = new Properties();
			info.put("url", JDBC_URL);
			info.put("user", USER);
			info.put("password", PASSWORD);
			connection = DriverManager.getConnection(
					"jdbc:accumulo:cosmos//localhost", info);
			statement = connection.createStatement();
			final ResultSet resultSet = statement
					.executeQuery("select \"PAGE_ID\" from \"sorts\".\""
							+ meataData.uuid() + "\"");

			final ResultSetMetaData metaData = resultSet.getMetaData();
			final int columnCount = metaData.getColumnCount();

			assertEquals(columnCount, 1);

			int resultsFound = 0;
			SortedSet<String> sets = Sets.newTreeSet();
			for(int i=0; i < 10; i++)
			{
				sets.add(Integer.valueOf(i).toString());	
			}
			Queue<String> values = Lists.newLinkedList(sets);
			
			while (resultSet.next()) {
				assertEquals(metaData.getColumnName(1), "PAGE_ID");
				List<Entry<Column, SValue>> sValues = (List<Entry<Column, SValue>>) resultSet
						.getObject("PAGE_ID");
				assertEquals(sValues.size(), 1);
				SValue onlyValue = sValues.iterator().next().getValue();
				assertEquals(onlyValue.visibility().toString(), "[en]");
				values.remove(onlyValue.value());
				resultsFound++;

			}

			assertEquals(resultsFound, 10);
			assertEquals(values.size(), 0);
		} finally {
			close(connection, statement);
		}
	}



	public static void loadCountsForRecord(Map<Column, Long> counts,
			MultimapQueryResult r) {
		for (Entry<Column, SValue> entry : r.columnValues()) {
			Column c = entry.getKey();
			if (counts.containsKey(c)) {
				counts.put(c, counts.get(c) + 1);
			} else {
				counts.put(c, 1l);
			}
		}
	}

	public static MultimapQueryResult pagesToQueryResult(Page p) {
		HashMultimap<Column, SValue> data = HashMultimap.create();

		String pageId = Long.toString(p.getId());

		data.put(PAGE_ID, SValue.create(pageId, cv));

		Revision r = p.getRevision();
		if (null != r) {
			data.put(REVISION_ID, SValue.create(Long.toString(r.getId()), cv));
			data.put(REVISION_TIMESTAMP, SValue.create(r.getTimestamp(), cv));

			Contributor c = r.getContributor();
			if (null != c) {
				if (null != c.getUsername()) {
					data.put(CONTRIBUTOR_USERNAME,
							SValue.create(c.getUsername(), cv));
				}

				if (0l != c.getId()) {
					data.put(CONTRIBUTOR_ID,
							SValue.create(Long.toString(c.getId()), cv));
				}
			}
		}

		return new MultimapQueryResult(data, pageId, cv);
	}

}
