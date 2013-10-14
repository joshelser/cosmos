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

import java.net.URL;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * 
 */
public class CosmosWebConfiguration {
  private static final Logger log = LoggerFactory.getLogger(CosmosWebConfiguration.class);
  
  public static final String TRACE_WEB_CONFIGURATION_FILE = "monitor.xml";
  
  public static final String ZOOKEEPERS = "cosmos.accumulo.zookeepers";
  public static final String ACCUMULO_INSTANCE = "cosmos.accumulo.instance";
  public static final String ACCUMULO_USER = "cosmos.accumulo.username";
  public static final String ACCUMULO_PASSWORD = "cosmos.accumulo.password";
  
  private static final Set<String> REQUIRED_OPTIONS = ImmutableSet.of(ZOOKEEPERS, ACCUMULO_INSTANCE, ACCUMULO_USER, ACCUMULO_PASSWORD);  
  
  private static Configuration conf = null;
  
  public synchronized static Configuration get() {
    if (null == conf) {
      loadConf();
    }
    
    return conf;
  }
  
  private static void loadConf() {
    conf = new Configuration(false);
    
    URL file = CosmosWebConfiguration.class.getClassLoader().getResource(TRACE_WEB_CONFIGURATION_FILE);
    if (null != file) {
      conf.addResource(file);
      
      validateConf();
      
      return;
    }
    
    throw new RuntimeException("Could not load 'monitor.xml' from classpath.");
  }
  
  private static void validateConf() {
    Set<String> required = Sets.newHashSet(REQUIRED_OPTIONS);
    for (Entry<String,String> entry : conf) {
      required.remove(entry.getKey());
      
      if (required.isEmpty()) {
        return;
      }
    }
    
    if (!required.isEmpty()) {
      String msg = "Following configuration options required in " + TRACE_WEB_CONFIGURATION_FILE + ": " + required.toString();
      log.error(msg);
      throw new RuntimeException(msg);
    }
  }
}
