/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cosmos.trace;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class AccumuloTraceStore {
  private static final Logger log = LoggerFactory.getLogger(AccumuloTraceStore.class);
  
  public static final String TABLE_NAME = "tracecosmos";
  public static final long BUFFER = 50 * 1024 * 1024l, LATENCY = 2 * 60 * 1000l;
  
  public static void ensureTables(Connector c) throws AccumuloException, AccumuloSecurityException {
    checkNotNull(c);
    
    TableOperations tops = c.tableOperations();
    if (!tops.exists(TABLE_NAME)) {
      try {
        tops.create(TABLE_NAME);
      } catch (TableExistsException e) {
        log.debug("Could not create trace table {}", TABLE_NAME);
        log.debug("Caught TableNotFoundException", e);
      }
    }
  }
  
  
  public static void serialize(Tracer t, Connector c) throws TableNotFoundException, MutationsRejectedException {
    checkNotNull(t);
    checkNotNull(c);
    
    BatchWriter bw = c.createBatchWriter(TABLE_NAME, BUFFER, LATENCY, 3);
    try {
      bw.addMutations(t.toMutations());
    } finally {
      bw.close();
    }
  }
}
