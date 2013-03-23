package sorts.results;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.UUID.randomUUID;

import org.apache.accumulo.core.client.Connector;

import sorts.options.Defaults;


public class SortableResult {
  
  protected final Connector connector;
  protected final String dataTable, metadataTable;
  protected final String UUID;
  
  public SortableResult(Connector connector) {
    this(connector, Defaults.DATA_TABLE, Defaults.METADATA_TABLE);
  }
  
  public SortableResult(Connector connector, String dataTable, String metadataTable) {
    checkNotNull(connector);
    checkNotNull(dataTable);
    checkNotNull(metadataTable);
    
    this.connector = connector;
    this.dataTable = dataTable;
    this.metadataTable = metadataTable;
    
    this.UUID = randomUUID().toString();
  }
  
}
