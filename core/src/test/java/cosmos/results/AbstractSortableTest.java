package cosmos.results;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;

import cosmos.options.Defaults;

/**
 * 
 */
public class AbstractSortableTest {
  protected static final ColumnVisibility VIZ = new ColumnVisibility("test");
  protected static final Authorizations AUTHS = new Authorizations("test");
  
  protected Connector c;
  protected TestingServer zk;
  
  @Before
  public void setup() throws Exception {
    MockInstance mi = new MockInstance();
    PasswordToken pw = new PasswordToken("");
    c = mi.getConnector("root", pw);
    c.securityOperations().changeUserAuthorizations("root", new Authorizations("test"));
    c.tableOperations().create(Defaults.DATA_TABLE);
    c.tableOperations().create(Defaults.METADATA_TABLE);
    
    zk = new TestingServer();
  }
  
  @After
  public void cleanup() throws Exception {
    if (null != zk) {
      zk.close();
    }
  }
  
  protected String zkConnectString() {
    return zk.getConnectString();
  }
}
