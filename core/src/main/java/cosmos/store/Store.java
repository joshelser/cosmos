/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 *  Copyright 2013 Josh Elser
 *
 */
package cosmos.store;

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
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;

import cosmos.options.Defaults;
import cosmos.options.Index;
import cosmos.results.Column;
import cosmos.trace.AccumuloTraceStore;
import cosmos.trace.Tracer;
import cosmos.util.IdentitySet;

public class Store {
  private static final Logger log = LoggerFactory.getLogger(Store.class);
  
  private static final SortedSet<Text> SPLITS = ImmutableSortedSet.of(new Text("0"), new Text("1"), new Text("2"), new Text("3"), new Text("4"), new Text("5"),
      new Text("6"), new Text("7"), new Text("8"), new Text("9"), new Text("a"), new Text("b"), new Text("c"), new Text("d"), new Text("e"), new Text("f"));
  
  protected final Connector connector;
  protected final Authorizations auths;
  protected final boolean lockOnUpdates;
  protected final String dataTable, metadataTable;
  protected final String UUID;
  protected final Tracer tracer;
  
  protected Set<Index> columnsToIndex;
  
  public Store(Connector connector, Authorizations auths, Set<Index> columnsToIndex) {
    this(connector, auths, randomUUID().toString(), columnsToIndex, Defaults.LOCK_ON_UPDATES, Defaults.DATA_TABLE, Defaults.METADATA_TABLE);
  }
  
  public Store(Connector connector, Authorizations auths, String uuid, Set<Index> columnsToIndex) {
    this(connector, auths, uuid, columnsToIndex, Defaults.LOCK_ON_UPDATES, Defaults.DATA_TABLE, Defaults.METADATA_TABLE);
  }
  
  public Store(Connector connector, Authorizations auths, Set<Index> columnsToIndex, boolean lockOnUpdates) {
    this(connector, auths, randomUUID().toString(), columnsToIndex, lockOnUpdates, Defaults.DATA_TABLE, Defaults.METADATA_TABLE);
  }
  
  public Store(Connector connector, Authorizations auths, String uuid, Set<Index> columnsToIndex, boolean lockOnUpdates) {
    this(connector, auths, uuid, columnsToIndex, lockOnUpdates, Defaults.DATA_TABLE, Defaults.METADATA_TABLE);
  }
  
  public Store(Connector connector, Authorizations auths, Set<Index> columnsToIndex, boolean lockOnUpdates, String dataTable, String metadataTable) {
    this(connector, auths, randomUUID().toString(), columnsToIndex, lockOnUpdates, dataTable, metadataTable);
  }
  
  public Store(Connector connector, Authorizations auths, String uuid, Set<Index> columnsToIndex, boolean lockOnUpdates, String dataTable, String metadataTable) {
    checkNotNull(connector);
    checkNotNull(auths);
    checkNotNull(uuid);
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
    
    this.UUID = uuid;
    
    this.tracer = new Tracer(uuid());
    
    TableOperations tops = this.connector.tableOperations();
    
    // A slight hack -- if we have something that isn't actually providing us
    // a valid tableOperations element, don't try to create/configure tables
    if (null != tops) {
      createIfNotExists(tops, this.dataTable());
      splitTable(tops, this.dataTable());
      addLocalityGroups(tops, this.dataTable());
      createIfNotExists(tops, this.metadataTable());
      ensureTracingTableExists();
    }
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
   * Make sure we have a reasonable number of splits for the data table or else concurrency will just grind to a halt.
   * 
   * @param tops
   * @param tableName
   */
  protected void splitTable(TableOperations tops, String tableName) {
    try {
      // Having 10 splits should be no problem on a single machine
      // so we can always split to that point
      final int EXPECTED_SPLITS = 10;
      Collection<Text> splits = tops.listSplits(tableName, EXPECTED_SPLITS);
      
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
    }
  }
  
  /**
   * Ensure that the {@link Defaults.CONTENT_LG_NAME} locality group is configured
   * 
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
    }
    
    try {
      Map<String,Set<Text>> localityGroups = tops.getLocalityGroups(tableName);
      
      // If we don't have a locality group specified with the expected name
      // create one automatically
      if (!localityGroups.containsKey(Defaults.DOCID_FIELD_NAME)) {
        localityGroups.put(Defaults.DOCID_FIELD_NAME, Collections.singleton(Defaults.DOCID_FIELD_NAME_TEXT));
        
        tops.setLocalityGroups(tableName, localityGroups);
      } else {
        Set<Text> colfams = localityGroups.get(Defaults.DOCID_FIELD_NAME);
        if (!colfams.contains(Defaults.DOCID_FIELD_NAME_TEXT)) {
          log.warn("The {} locality group does not contain the expected column family {}", Defaults.DOCID_FIELD_NAME, Defaults.DOCID_FIELD_NAME_TEXT);
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
    }
    
    Set<Index> columns = columnsToIndex();
    if (!(columns instanceof IdentitySet)) {
      try {
        Map<String,Set<Text>> localityGroups = tops.getLocalityGroups(tableName);
        
        for (Index index : columns) {
          Column c = index.column();
          String columnName = c.name();
          Text textColumnName = new Text(columnName);
          
          Set<Text> colfams;
          if (localityGroups.containsKey(columnName)) {
            colfams = localityGroups.get(columnName);
            
            // We already have the colfam defined
            if (colfams.contains(textColumnName)) {
              continue;
            }
          } else {
            colfams = Sets.newHashSet();
          }
          
          colfams.add(textColumnName);
          
          localityGroups.put(c.name(), colfams);
        }
        
        tops.setLocalityGroups(tableName, localityGroups);
        
      } catch (AccumuloException e) {
        log.error("Could not add locality groups to table '{}'", tableName, e);
        throw new RuntimeException(e);
      } catch (AccumuloSecurityException e) {
        log.error("Could not add locality groups to table '{}'", tableName, e);
        throw new RuntimeException(e);
      } catch (TableNotFoundException e) {
        log.error("Could not add locality groups to table '{}'", tableName, e);
        throw new RuntimeException(e);
      }
    }
  }
  
  protected void ensureTracingTableExists() {
    try {
      AccumuloTraceStore.ensureTables(connector());
    } catch (AccumuloException e) {
      throw new RuntimeException(e);
    } catch (AccumuloSecurityException e) {
      throw new RuntimeException(e);
    }
    
  }
  
  /**
   * Given some {@link Index}s, we can assign locality groups to make queries over those columns more efficient, especially for operations like groupBy.
   * 
   * @param indices
   * @throws AccumuloSecurityException
   * @throws TableNotFoundException
   * @throws AccumuloException
   */
  public void optimizeIndices(Iterable<Index> indices) throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
    Preconditions.checkNotNull(indices);
    
    final TableOperations tops = connector().tableOperations();
    
    Map<String,Set<Text>> locGroups = tops.getLocalityGroups(dataTable());
    int size = locGroups.size();
    
    // Take the set of indicies that were requested to be "optimized" (read as locality group'ed)
    for (Index i : indices) {
      String colf = i.column().name();
      // And update our mapping accordingly
      if (!locGroups.containsKey(colf)) {
        locGroups.put(colf, Sets.newHashSet(new Text(colf)));
      }
    }
    
    // If we've actually added some locality groups, set them back on the table
    if (size != locGroups.size()) {
      log.debug("Setting {} new locality groups", locGroups.size() - size);
      tops.setLocalityGroups(dataTable(), locGroups);
    } else {
      log.debug("No new locality groups to set");
    }
  }
  
  /**
   * Issues a compaction for the range of data contained in this SortableResult
   * 
   * @throws AccumuloSecurityException
   * @throws TableNotFoundException
   * @throws AccumuloException
   */
  public void consolidate() throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
    consolidate(true);
  }
  
  /**
   * Issues a compaction for the range od data contained in this SortableResult, optionally issuing a flush first
   * 
   * @param flush
   *          Whether or not to flush the data sitting in memory before compaction
   * @throws AccumuloSecurityException
   * @throws TableNotFoundException
   * @throws AccumuloException
   */
  public void consolidate(boolean flush) throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
    connector().tableOperations().compact(dataTable(), new Text(uuid()), new Text(uuid() + Defaults.EIN_BYTE_STR), flush, true);
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
  
  public Tracer tracer() {
    return this.tracer;
  }
  
  public void sendTraces() {
    try {
      AccumuloTraceStore.serialize(tracer(), connector());
    } catch (MutationsRejectedException e) {
      log.debug("Could not persist trace information", e);
    } catch (TableNotFoundException e) {
      log.debug("Could not persist trace information", e);
    }
  }
  
  public void addColumnsToIndex(Collection<Index> columns) {
    checkNotNull(columns);
    
    if (IdentitySet.class.isAssignableFrom(columns.getClass())) {
      // We got an IdentitySet, so we're now an IdentitySet
      this.columnsToIndex = (IdentitySet<Index>) columns;
    } else if (!(IdentitySet.class.isAssignableFrom(this.columnsToIndex.getClass()))) {
      // We aren't already an IdentitySet
      this.columnsToIndex.addAll(columns);
    }
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof Store) {
      Store other = (Store) o;
      
      // There's no equality check on the connector implementations, which makes this
      // check very brittle. Not to mention, you could have multiple "equivalent" connectors
      // to the same Instance. Even using the Instance (name + zookeepers) isn't perfect because
      // you could have subset zookeepers declared that would still point to the same instance..
      //
      // So -- I'm going to be lazy right now
      //
      // if (!connector().equals(other.connector())) {
      // return false;
      // }
      
      if (!auths().equals(other.auths()) || !uuid().equals(other.uuid()) || lockOnUpdates() != other.lockOnUpdates() || !dataTable().equals(other.dataTable())
          || !metadataTable().equals(other.metadataTable())) {
        return false;
      }
      
      Set<Index> ourIndex = columnsToIndex(), theirIndex = other.columnsToIndex();
      
      // Try to unravel the confusion in multiple implementations of Set that I created
      if (ourIndex.equals(theirIndex)) {
        return true;
      } else if (IdentitySet.class.isAssignableFrom(ourIndex.getClass()) && IdentitySet.class.isAssignableFrom(theirIndex.getClass())) {
        // We're both IdentitySets -- contents of the "Set" doesn't matter, just that they're the same
        // implementation (assuming their implementation of contains(Object) is what uniquely identifies the class)
        return ourIndex.getClass().equals(theirIndex.getClass());
        
      } else if (!IdentitySet.class.isAssignableFrom(ourIndex.getClass()) && !IdentitySet.class.isAssignableFrom(theirIndex.getClass())) {
        // They're both not IdentitySets, so we assume that they're both "regular" concrete Set impls (TreeSet, HashSet, etc)
        return ourIndex.equals(theirIndex);
      }
    }
    
    return false;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(256);
    sb.append("Store:").append(uuid()).append(",").append(dataTable()).append(",").append(metadataTable());
    return sb.toString();
  }
  
  public static Store create(Connector connector, Authorizations auths, Set<Index> columnsToIndex) {
    return new Store(connector, auths, columnsToIndex);
  }
  
  public static Store create(Connector connector, Authorizations auths, String uuid, Set<Index> columnsToIndex) {
    return new Store(connector, auths, uuid, columnsToIndex);
  }
  
  public static Store create(Connector connector, Authorizations auths, Set<Index> columnsToIndex, boolean lockOnUpdates) {
    return new Store(connector, auths, columnsToIndex, lockOnUpdates);
  }
  
  public static Store create(Connector connector, Authorizations auths, String uuid, Set<Index> columnsToIndex, boolean lockOnUpdates) {
    return new Store(connector, auths, uuid, columnsToIndex, lockOnUpdates);
  }
  
  public static Store create(Connector connector, Authorizations auths, Set<Index> columnsToIndex, boolean lockOnUpdates, String dataTable, String metadataTable) {
    return new Store(connector, auths, columnsToIndex, lockOnUpdates, dataTable, metadataTable);
  }
  
  public static Store create(Connector connector, Authorizations auths, String uuid, Set<Index> columnsToIndex, boolean lockOnUpdates, String dataTable,
      String metadataTable) {
    return new Store(connector, auths, uuid, columnsToIndex, lockOnUpdates, dataTable, metadataTable);
  }
  
}
