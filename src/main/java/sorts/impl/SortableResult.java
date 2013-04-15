package sorts.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.UUID.randomUUID;

import java.util.Collection;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

import sorts.options.Defaults;
import sorts.options.Index;
import sorts.util.IdentitySet;

import com.google.common.collect.Sets;

public class SortableResult {
  
  protected final Connector connector;
  protected final Authorizations auths;
  protected final Set<Index> columnsToIndex;
  protected final String dataTable, metadataTable;
  protected final String UUID;
  
  public SortableResult(Connector connector, Authorizations auths, Set<Index> columnsToIndex) {
    this(connector, auths, columnsToIndex, Defaults.DATA_TABLE, Defaults.METADATA_TABLE);
  }
  
  public SortableResult(Connector connector, Authorizations auths, Set<Index> columnsToIndex, String dataTable, String metadataTable) {
    checkNotNull(connector);
    checkNotNull(auths);
    checkNotNull(columnsToIndex);
    checkNotNull(dataTable);
    checkNotNull(metadataTable);
    
    this.connector = connector;
    this.auths = auths;
    this.columnsToIndex = Sets.newHashSet(columnsToIndex);
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
  
  public Set<Index> columnsToIndex() {
    return this.columnsToIndex;
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
  
  protected void addColumnsToIndex(Collection<Index> columns) {
    checkNotNull(columns);
    
    if (!(this.columnsToIndex instanceof IdentitySet)) {
      this.columnsToIndex.addAll(columns);
    }
  }
  
  public static SortableResult create(Connector connector, Authorizations auths, Set<Index> columnsToIndex) {
    return new SortableResult(connector, auths, columnsToIndex);
  }
  
  public static SortableResult create(Connector connector, Authorizations auths, Set<Index> columnsToIndex, String dataTable, String metadataTable) {
    return new SortableResult(connector, auths, columnsToIndex, dataTable, metadataTable);
  }
  
}
