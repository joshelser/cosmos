
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.io.Files;

import cosmos.util.sql.AccumuloDriver;


public class TestSql {

	
	 protected static File tmp = Files.createTempDir();
	  protected static MiniAccumuloCluster mac;
	  protected static MiniAccumuloConfig macConfig;
	  
	  @BeforeClass
	  public static void setup() throws IOException, InterruptedException {
	    macConfig = new MiniAccumuloConfig(tmp, "root");
	    mac = new MiniAccumuloCluster(macConfig);
	    
	    mac.start();
	    
	    // Do this now so we don't forget later (or get an exception)
	    tmp.deleteOnExit();
	  }
	  
	  @AfterClass
	  public static void teardown() throws IOException, InterruptedException {
	    mac.stop();
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
	public void test()
	{
		// replace test until I can stop this test from relying on my local store
	}
	@Ignore
	@Test
	public void testVanityDriver() throws SQLException {
		loadDriverClass();
	    Connection connection = null;
	    Statement statement = null;
	    try {
	      Properties info = new Properties();
	      info.put("url", JDBC_URL);
	      info.put("user", USER);
	      info.put("password", PASSWORD);
	      connection = DriverManager.getConnection("jdbc:splunk://localhost", info);
	      statement = connection.createStatement();
	      final ResultSet resultSet =
	          statement.executeQuery(
	        		  
	              "select \"PAGE_ID\" from \"sorts\".\"sorts\"  where \"REVISION_ID\" = '1522908'");
	      
	      /*
	       * 
            "select * from (\n"
            + "  select * from \"sales_fact_1997\"\n"
            + "  union all\n"
            + "  select * from \"sales_fact_1998\")\n"
            + "where \"product_id\" = 1"
	       */
	      output(resultSet, System.out);
	    } finally {
	      close(connection, statement);
	    }
	}
	
	@Ignore
	@Test
	public void testVanityDriver2() throws SQLException {
		loadDriverClass();
	    Connection connection = null;
	    Statement statement = null;
	    try {
	      Properties info = new Properties();
	      info.put("url", JDBC_URL);
	      info.put("user", USER);
	      info.put("password", PASSWORD);
	      connection = DriverManager.getConnection("jdbc:splunk://localhost", info);
	      statement = connection.createStatement();
	      final ResultSet resultSet =
	          statement.executeQuery(
	        		  
	              "select \"tart\" from \"sorts\".\"sorts\" where \"tart\" = 'asdfs'");
	      
	      /*
	       * 
            "select * from (\n"
            + "  select * from \"sales_fact_1997\"\n"
            + "  union all\n"
            + "  select * from \"sales_fact_1998\")\n"
            + "where \"product_id\" = 1"
	       */
	      output(resultSet, System.out);
	    } finally {
	      close(connection, statement);
	    }
	}
	
	
	
	
	private void output(
		      ResultSet resultSet, PrintStream out) throws SQLException {
		    final ResultSetMetaData metaData = resultSet.getMetaData();
		    final int columnCount = metaData.getColumnCount();
		    while (resultSet.next()) {
		    	System.out.println("another result");
		      for (int i = 1;; i++) {
		        out.print(resultSet.getString(i));
		        if (i < columnCount) {
		          out.print(", ");
		        } else {
		          out.println();
		          break;
		        }
		      }
		    }
		  }

}
