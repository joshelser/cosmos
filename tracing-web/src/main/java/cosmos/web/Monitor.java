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
package cosmos.web;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.hadoop.conf.Configuration;

/**
 * 
 */
public class Monitor {
  
  public static final String ZOOKEEPERS = "zookeepers",
      INSTANCE = "instance", USERNAME = "username", PASSWORD = "password";
  
  
  protected static Connector connector;
  
  protected static void setConnector(Configuration conf) throws AccumuloException, AccumuloSecurityException {
    /*String zk = conf.get(ZOOKEEPERS);
    String instanceName = conf.get(INSTANCE);
    String username = conf.get(USERNAME);
    String password = conf.get(PASSWORD);*/
    
    ZooKeeperInstance instance = new ZooKeeperInstance("accumulo1.5", "localhost");
    connector = instance.getConnector("root", "secret".getBytes()); 
  }
  
  public static Connector getConnector() {
    return connector;
  } 
}
