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
package cosmos.results;

import java.util.Collections;

import org.apache.accumulo.core.Constants;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import cosmos.Cosmos;
import cosmos.SortableMetadata;
import cosmos.SortableMetadata.State;
import cosmos.impl.CosmosImpl;
import cosmos.impl.SortableResult;
import cosmos.options.Index;

/**
 * 
 */
@RunWith(JUnit4.class)
public class SortableResultStateTest extends AbstractSortableTest {
  
  @Test
  public void test() throws Exception {
    SortableResult id = SortableResult.create(c, Constants.NO_AUTHS, Collections.<Index> emptySet());
    
    Assert.assertEquals(State.UNKNOWN, SortableMetadata.getState(id));
        
    Cosmos s = new CosmosImpl(zk.getConnectString());
    s.register(id);
    
    Assert.assertEquals(State.LOADING, SortableMetadata.getState(id));
    
    s.finalize(id);
    
    Assert.assertEquals(State.LOADED, SortableMetadata.getState(id));
    
    // Would be State.DELETING during this call
    s.delete(id);
    
    Assert.assertEquals(State.UNKNOWN, SortableMetadata.getState(id));
    
    s.close();
  }
  
}
