/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
 */
package cosmos.util.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;

import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.impl.java.MapSchema;
import net.hydromatic.optiq.impl.jdbc.JdbcSchema;
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
import cosmos.options.Order;
import cosmos.results.Column;
import cosmos.results.SValue;
import cosmos.results.impl.MultimapQueryResult;
import cosmos.util.sql.impl.CosmosTable;

/**
 * JDBC driver for Splunk.
 * 
 * <p>
 * It accepts connect strings that start with "jdbc:splunk:".
 * </p>
 */
public class AccumuloDriver extends UnregisteredDriver {
	public static final ColumnVisibility cv = new ColumnVisibility("en");
	final Random offsetR = new Random(), cardinalityR = new Random();

	private int recordsReturned;
	protected AccumuloDriver() {
		super();
	}

	static {
		new AccumuloDriver().register();
	}

	protected String getConnectStringPrefix() {
		return "jdbc:splunk:";
	}

	protected DriverVersion createDriverVersion() {
		return new AccumuloJdbcDriverVersion();
	}

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

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		Connection connection = super.connect(url, info);
		OptiqConnection optiqConnection = (OptiqConnection) connection;

		final MutableSchema rootSchema = optiqConnection.getRootSchema();
		final String schemaName = "sorts";

		// optiqConnection.setSchema("");

		ZooKeeperInstance instance = new ZooKeeperInstance("accumulo",
				"dev01:2181");
		try {
			Connector connector = instance.getConnector("root",
					new PasswordToken("secret"));

			Set<Index> columns = Sets.newHashSet();
			columns.add(new Index(PAGE_ID));
			columns.add(new Index(REVISION_ID));
			columns.add(new Index(REVISION_TIMESTAMP));
			columns.add(new Index(CONTRIBUTOR_ID));

			AccumuloSchema schema;
			// /optiqConnection.setSchema("sorts");
			// rootSchema.getSubSchemaExpression(schemaName, Schema.class)

			SortableResult meataData = new SortableResult(connector, connector
					.securityOperations().getUserAuthorizations("root"),
					columns, false);

			int offset = offsetR.nextInt(MAX_OFFSET);
			int numRecords = cardinalityR.nextInt(MAX_SIZE);

			BatchScanner bs = connector.createBatchScanner("sortswiki",
					new Authorizations(), 4);

			bs.setRanges(Collections.singleton(new Range(Integer
					.toString(offset), Integer.toString(MAX_ROW))));

			Iterable<Entry<Key, Value>> inputIterable = Iterables.limit(bs,
					numRecords);

			
			recordsReturned = 0;
			CosmosImpl impl = new CosmosImpl(
					"localhost");
			CosmosSql cosmosSql = new CosmosSql(meataData,impl );
			
			
			
			
			Function<Entry<Key,Value>,MultimapQueryResult> func = new Function<Entry<Key,Value>,MultimapQueryResult>() {
		        @Override
		        public MultimapQueryResult apply(Entry<Key,Value> input) {
		          Page p;
		          try {
		            p = Page.parseFrom(input.getValue().get());
		          } catch (InvalidProtocolBufferException e) {
		            throw new RuntimeException(e);
		          }
		          return pagesToQueryResult(p);
		        }
		      };
		      
		      Map<Column,Long> counts = Maps.newHashMap();
		      ArrayList<MultimapQueryResult> tformSource = Lists.newArrayListWithCapacity(20000);
		      
		      Stopwatch sw = new Stopwatch();
		      Stopwatch tformSw = new Stopwatch();
		      
		      for (Entry<Key,Value> input : inputIterable) {
		        tformSw.start();
		        
		        MultimapQueryResult r = func.apply(input);
		        tformSource.add(r);
		        
		        tformSw.stop();
		        
		        loadCountsForRecord(counts, r);
		        recordsReturned++;
		      }
		      
		      sw.start();
		      impl.addResults(meataData, tformSource);

			schema = new AccumuloSchema<CosmosSql>(rootSchema, "sorts",
					"sorts", "sorts", rootSchema.getSubSchemaExpression(
							schemaName, AccumuloSchema.class), cosmosSql,
					CosmosTable.class);

			schema.initialize();
			rootSchema.addSchema(schemaName, schema);

			Map<String, Object> users = Maps.newHashMap();
			users.put("admin", "changeme");

			BasicDataSource dataSource = new BasicDataSource();
			dataSource.setUrl(getConnectStringPrefix() + "//localhost");
			dataSource.setUsername("admin");
			dataSource.setPassword("changeme");

			// JdbcSchema.create(rootSchema,dataSource,"admin","",schemaName);

		} catch (AccumuloException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (AccumuloSecurityException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnexpectedStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return optiqConnection;
	}
	
	 public void loadCountsForRecord(Map<Column,Long> counts, MultimapQueryResult r) {
		  for (Entry<Column,SValue> entry : r.columnValues()) {
			  Column c = entry.getKey();
			  if (counts.containsKey(c)) {
				  counts.put(c, counts.get(c)+1);
			  } else {
				  counts.put(c, 1l);
			  }
		  }
	  }
	 
	  public static MultimapQueryResult pagesToQueryResult(Page p) {
		    HashMultimap<Column,SValue> data = HashMultimap.create();
		    
		    String pageId = Long.toString(p.getId());
		    
		    data.put(PAGE_ID, SValue.create(pageId, cv));
		    
		    Revision r = p.getRevision();
		    if (null != r) {
		      data.put(REVISION_ID, SValue.create(Long.toString(r.getId()), cv));
		      data.put(REVISION_TIMESTAMP, SValue.create(r.getTimestamp(), cv));
		      
		      Contributor c = r.getContributor();
		      if (null != c) {
		        if (null != c.getUsername()) {
		          data.put(CONTRIBUTOR_USERNAME, SValue.create(c.getUsername(), cv));
		        }
		        
		        if (0l != c.getId()) {
		          data.put(CONTRIBUTOR_ID, SValue.create(Long.toString(c.getId()), cv));
		        }
		      }
		    }
		    
		    return new MultimapQueryResult(data, pageId, cv);
		  }
}

// End SplunkDriver.java
