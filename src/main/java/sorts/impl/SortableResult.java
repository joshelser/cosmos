package sorts.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.UUID.randomUUID;

import java.util.Collection;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sorts.options.Defaults;
import sorts.options.Index;
import sorts.util.IdentitySet;

import com.google.common.collect.Sets;

public class SortableResult {
  private static final Logger log = LoggerFactory.getLogger(SortableResult.class);
  
  protected final Connector connector;
  protected final Authorizations auths;
  protected final boolean lockOnUpdates;
  protected final String dataTable, metadataTable;
  protected final String UUID;
  
  protected Set<Index> columnsToIndex;
  
  public SortableResult(Connector connector, Authorizations auths, Set<Index> columnsToIndex) {
    this(connector, auths, columnsToIndex, Defaults.LOCK_ON_UPDATES, Defaults.DATA_TABLE, Defaults.METADATA_TABLE);
  }
  
  public SortableResult(Connector connector, Authorizations auths, Set<Index> columnsToIndex, boolean lockOnUpdates) {
    this(connector, auths, columnsToIndex, lockOnUpdates, Defaults.DATA_TABLE, Defaults.METADATA_TABLE);
  }
  
  public SortableResult(Connector connector, Authorizations auths, Set<Index> columnsToIndex, boolean lockOnUpdates, String dataTable, String metadataTable) {
    checkNotNull(connector);
    checkNotNull(auths);
    checkNotNull(columnsToIndex);
    checkNotNull(dataTable);
    checkNotNull(metadataTable);
    
    this.connector = connector;
    this.auths = auths;
    this.lockOnUpdates = lockOnUpdates;
    
    // Make sure we don't try to make a real Set out of the IdentitySet
    if (columnsToIndex instanceof IdentitySet) {
      this.columnsToIndex = columnsToIndex; 
    } else {
      this.columnsToIndex = Sets.newHashSet(columnsToIndex);
    }
    
    this.dataTable = dataTable;
    this.metadataTable = metadataTable;
    
    this.UUID = randomUUID().toString();
    
    TableOperations tops = this.connector.tableOperations();
    
    createIfNotExists(tops, this.dataTable());
    createIfNotExists(tops, this.metadataTable());
  }
  
  protected void createIfNotExists(TableOperations tops, String tableName) {
    if (!tops.exists(this.dataTable)) {
      try {
        tops.create(this.dataTable);
        
        //TODO Make a better API than runtimeexception? Do (should) I care?
        // If the user we were given can't do what's necessary, then
        // it needed to be done ahead of time. Either way it's fatal?
        // I suppose best to just make a named-exception then so people
        // specifically know what happened.
      } catch (AccumuloException e) {
        log.error("Could not create table '{}'", this.dataTable, e);
        throw new RuntimeException(e);
      } catch (AccumuloSecurityException e) {
        log.error("Could not create table '{}'", this.dataTable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("Could not create table '{}'", this.dataTable, e);
        throw new RuntimeException(e);
      } 
    }
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
  
  public boolean lockOnUpdates() {
    return this.lockOnUpdates;
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
    
    if (IdentitySet.class.isAssignableFrom(columns.getClass())) {
      // We got an IdentitySet, so we're now an IdentitySet
      this.columnsToIndex = (IdentitySet<Index>) columns;
    } else if (!(IdentitySet.class.isAssignableFrom(this.columnsToIndex.getClass()))) {
      // We aren't already an IdentitySet
      this.columnsToIndex.addAll(columns);
    }
  }
  
  public static SortableResult create(Connector connector, Authorizations auths, Set<Index> columnsToIndex) {
    return new SortableResult(connector, auths, columnsToIndex);
  }
  
  public static SortableResult create(Connector connector, Authorizations auths, Set<Index> columnsToIndex, boolean lockOnUpdates) {
    return new SortableResult(connector, auths, columnsToIndex, lockOnUpdates);
  }
  
  public static SortableResult create(Connector connector, Authorizations auths, Set<Index> columnsToIndex, boolean lockOnUpdates, String dataTable, String metadataTable) {
    return new SortableResult(connector, auths, columnsToIndex, lockOnUpdates, dataTable, metadataTable);
  }
  
}
