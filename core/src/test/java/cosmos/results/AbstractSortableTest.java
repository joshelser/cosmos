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

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;

import cosmos.options.Defaults;

/**
 * 
 */
public class AbstractSortableTest {
  protected static final ColumnVisibility VIZ = new ColumnVisibility("test");
  protected static final Authorizations AUTHS = new Authorizations("test");
  
  protected Connector c;
  protected TestingServer zk;
  
  @Before
  public void setup() throws Exception {
    MockInstance mi = new MockInstance();
    PasswordToken pw = new PasswordToken("");
    c = mi.getConnector("root", pw);
    c.securityOperations().changeUserAuthorizations("root", new Authorizations("test"));
    c.tableOperations().create(Defaults.DATA_TABLE);
    c.tableOperations().create(Defaults.METADATA_TABLE);
    
    zk = new TestingServer();
  }
  
  @After
  public void cleanup() throws Exception {
    if (null != zk) {
      zk.close();
    }
  }
  
  protected String zkConnectString() {
    return zk.getConnectString();
  }
}
