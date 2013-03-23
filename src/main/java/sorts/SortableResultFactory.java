package sorts;
import org.apache.accumulo.core.client.Connector;




public class SortableResultFactory {
  
  public SortableResultFactory() {}
  
  public static SortableResult create(Connector connector) {
    return new SortableResult(connector);
  }
  
  public static SortableResult create(Connector connector, String dataTable, String metadataTable) {
    return new SortableResult(connector, dataTable, metadataTable);
  }
}
