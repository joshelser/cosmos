import org.apache.accumulo.core.client.Connector;

import com.google.common.base.Preconditions;


public class SortingFactory {
  private final Connector connector;
  
  public SortingFactory(Connector connector) {
    Preconditions.checkNotNull(connector);
    
    this.connector = connector;
  }
  
  public Sorting create() {
    return new SortingImpl(this.connector);
  }
}
