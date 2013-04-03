package sorts.results;

import java.util.Properties;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Before;

import sorts.options.Defaults;

/**
 * 
 */
public class AbstractSortableTest {
  
  protected Connector c;
  
  @Before
  public void setup() throws Exception {
    MockInstance mi = new MockInstance();
    Properties p = new Properties();
    p.setProperty("password", "");
    c = mi.getConnector("root", p);
    c.securityOperations().changeUserAuthorizations("root", new Authorizations("test"));
    c.tableOperations().create(Defaults.DATA_TABLE);
    c.tableOperations().create(Defaults.METADATA_TABLE);
  }
  
}
