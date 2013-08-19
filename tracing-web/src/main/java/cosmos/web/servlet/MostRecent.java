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
package cosmos.web.servlet;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cosmos.trace.Timings.TimedRegions;
import cosmos.trace.TracerClient;
import cosmos.web.Monitor;

/**
 * 
 */
public class MostRecent extends HttpServlet {
  private static final long serialVersionUID = 6085901598122922382L;
  private static final Logger log = LoggerFactory.getLogger(MostRecent.class);
  
  protected TracerClient tc;
  
  public MostRecent() {
    tc = new TracerClient(Monitor.getConnector());
  }
  
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String numRecentOpt = req.getParameter("num");
    int numRecent = 10;
    
    if (null == numRecentOpt) {
      try {
        numRecent = Integer.parseInt(numRecentOpt);
      } catch (NumberFormatException e) {
        log.error("Could not parse option {}, using default of {}", numRecentOpt, numRecent);
      }
    }
    
    List<TimedRegions> timings = tc.mostRecentTimings(numRecent);
    StringBuilder sb = new StringBuilder(256);
    sb.append("<html>");
    sb.append("<ul>");
    for (TimedRegions timing : timings) {
      sb.append("<li>").append(timing.toString()).append("</li>");
    }
    sb.append("</ul>");
    sb.append("</html>");
    
    resp.getWriter().append(sb.toString());
  }
}
