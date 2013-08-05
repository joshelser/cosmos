package cosmos.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.UUID.randomUUID;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;

import cosmos.options.Defaults;
import cosmos.options.Index;
import cosmos.util.IdentitySet;

public class SortableResult {
  private static final Logger log = LoggerFactory.getLogger(SortableResult.class);
  
  private static final SortedSet<Text> SPLITS = ImmutableSortedSet.of(new Text("0"), new Text("1"), new Text("2"), new Text("3"), new Text("4"), new Text("5"),
      new Text("6"), new Text("7"), new Text("8"), new Text("9"));
  
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
    splitTable(tops, this.dataTable());
    addLocalityGroups(tops, this.dataTable());
    createIfNotExists(tops, this.metadataTable());
  }
  
  protected void createIfNotExists(TableOperations tops, String tableName) {
    if (!tops.exists(tableName)) {
      try {
        tops.create(tableName);
        
        // TODO Make a better API than runtimeexception? Do (should) I care?
        // If the user we were given can't do what's necessary, then
        // it needed to be done ahead of time. Either way it's fatal?
        // I suppose best to just make a named-exception then so people
        // specifically know what happened.
      } catch (AccumuloException e) {
        log.error("Could not create table '{}'", tableName, e);
        throw new RuntimeException(e);
      } catch (AccumuloSecurityException e) {
        log.error("Could not create table '{}'", tableName, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("Could not create table '{}'", tableName, e);
        throw new RuntimeException(e);
      }
    }
  }
  
  /**
   * Make sure we have a reasonable number of splits for the data table
   * or else concurrency will just grind to a halt. 
   * @param tops
   * @param tableName
   */
  protected void splitTable(TableOperations tops, String tableName) {
    try {
      // Having 10 splits should be no problem on a single machine
      // so we can always split to that point
      final int EXPECTED_SPLITS = 10;
      Collection<Text> splits = tops.getSplits(tableName, EXPECTED_SPLITS);
      
      if (splits.size() < EXPECTED_SPLITS) {
        tops.addSplits(tableName, SPLITS);
      }
    } catch (TableNotFoundException e) {
      log.error("Could not add splits to table '{}'", tableName, e);
      throw new RuntimeException(e);
    } catch (AccumuloException e) {
      log.error("Could not add splits to table '{}'", tableName, e);
      throw new RuntimeException(e);
    } catch (AccumuloSecurityException e) {
      log.error("Could not add splits to table '{}'", tableName, e);
      throw new RuntimeException(e);
    } catch (NotImplementedException e) {
      log.warn("Could not add splits to MockInstance");
    }
  }
 
  /**
   * Ensure that the {@link Defaults.CONTENT_LG_NAME} locality group is configured
   * @param tops
   * @param tableName
   */
  protected void addLocalityGroups(TableOperations tops, String tableName) {
    try {
      Map<String,Set<Text>> localityGroups = tops.getLocalityGroups(tableName);
      
      // If we don't have a locality group specified with the expected name
      // create one automatically
      if (!localityGroups.containsKey(Defaults.CONTENTS_LG_NAME)) {
        localityGroups.put(Defaults.CONTENTS_LG_NAME, Collections.singleton(Defaults.CONTENTS_COLFAM_TEXT));
        
        tops.setLocalityGroups(tableName, localityGroups);
      } else {
        Set<Text> colfams = localityGroups.get(Defaults.CONTENTS_LG_NAME);
        if (!colfams.contains(Defaults.CONTENTS_COLFAM_TEXT)) {
          log.warn("The {} locality group does not contain the expected column family {}", Defaults.CONTENTS_LG_NAME, Defaults.CONTENTS_COLFAM_TEXT);
        }
      }
    } catch (AccumuloException e) {
      log.error("Could not add locality groups to table '{}'", tableName, e);
      throw new RuntimeException(e);
    } catch (AccumuloSecurityException e) {
      log.error("Could not add locality groups to table '{}'", tableName, e);
      throw new RuntimeException(e);
    } catch (TableNotFoundException e) {
      log.error("Could not add locality groups to table '{}'", tableName, e);
      throw new RuntimeException(e);
    } catch (NotImplementedException e) {
      log.warn("Could not interact with locality groups in MockInstance");
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
  
  public static SortableResult create(Connector connector, Authorizations auths, Set<Index> columnsToIndex, boolean lockOnUpdates, String dataTable,
      String metadataTable) {
    return new SortableResult(connector, auths, columnsToIndex, lockOnUpdates, dataTable, metadataTable);
  }
  
}
