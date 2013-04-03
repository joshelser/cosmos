package sorts;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.UUID.randomUUID;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

import sorts.options.Defaults;

public class SortableResult {
  
  protected final Connector connector;
  protected final Authorizations auths;
  protected final String dataTable, metadataTable;
  protected final String UUID;
  
  public SortableResult(Connector connector, Authorizations auths) {
    this(connector, auths, Defaults.DATA_TABLE, Defaults.METADATA_TABLE);
  }
  
  public SortableResult(Connector connector, Authorizations auths, String dataTable, String metadataTable) {
    checkNotNull(connector);
    checkNotNull(dataTable);
    checkNotNull(metadataTable);
    
    this.connector = connector;
    this.auths = auths;
    this.dataTable = dataTable;
    this.metadataTable = metadataTable;
    
    this.UUID = randomUUID().toString();
  }
  
  public Connector connector() {
    return this.connector;
  }
  
  public Authorizations auths() {
    return this.auths;
  }
  
  public String dataTable() {
    return this.dataTable;
  }
  
  public String metadataTable() {
    return this.metadataTable;
  }
  
  public String uuid() {
    return this.UUID;
  }
  
  public static SortableResult create(Connector connector, Authorizations auths) {
    return new SortableResult(connector, auths);
  }
  
  public static SortableResult create(Connector connector, Authorizations auths, String dataTable, String metadataTable) {
    return new SortableResult(connector, auths, dataTable, metadataTable);
  }
  
}
