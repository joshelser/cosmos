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

import javax.servlet.http.HttpServlet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.SessionHandler;

import cosmos.web.servlet.MostRecent;

/**
 * 
 */
public class Monitor {
  
  public static final String ZOOKEEPERS = "zookeepers",
      INSTANCE = "instance", USERNAME = "username", PASSWORD = "password";
  
  protected Server server;
  protected ContextHandlerCollection handler;
  protected Context root;
  protected SocketConnector sock;
  
  protected static Connector connector;
  
  protected static void setConnector(Configuration conf) throws AccumuloException, AccumuloSecurityException {
    /*String zk = conf.get(ZOOKEEPERS);
    String instanceName = conf.get(INSTANCE);
    String username = conf.get(USERNAME);
    String password = conf.get(PASSWORD);*/
    
    ZooKeeperInstance instance = new ZooKeeperInstance("accumulo1.5", "localhost");
    connector = instance.getConnector("root", new PasswordToken("secret")); 
  }
  
  public static Connector getConnector() {
    return connector;
  }
  
  public Monitor() {
    server = new Server();
    handler = new ContextHandlerCollection();
    root = new Context(handler, "/", new SessionHandler(), null, null, null);
    
    sock = new SocketConnector();
    sock.setHost("127.0.0.1");
    sock.setPort(8080);

    server.addConnector(sock);
    server.setHandler(handler);
  }
  
  public void addServlet(Class<? extends HttpServlet> clz, String path) {
    root.addServlet(clz, path);
  }
  
  public void start() throws Exception {
    server.start();
  }
  
  public static void main(String[] args) throws Exception {
    /*if (1 != args.length) {
      System.err.println("Configuration file required as argument to Monitor");
      System.exit(1);
    }
    
    LocalFileSystem fs = new LocalFileSystem();
    Configuration conf = new Configuration();
    Path p = new Path(args[0]);
    if (!fs.exists(p)) {
      System.err.println("Could not locate file on local filesystem: " + p);
      System.exit(1);
    }
    
    conf.addResource(p);*/
    
    Monitor.setConnector(null);
    
    Monitor m = new Monitor();
    
    m.addServlet(MostRecent.class, "/");
    
    m.start();
  }
}
