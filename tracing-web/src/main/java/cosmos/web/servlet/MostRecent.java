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

import java.util.List;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cosmos.trace.Timings.TimedRegions;
import cosmos.trace.TracerClient;

/**
 * 
 */
@Path("/cosmos")
public class MostRecent {
  private static final Logger log = LoggerFactory.getLogger(MostRecent.class);
  
  protected static TracerClient tc;
  
  static {
    try {
      tc = new TracerClient("accumulo1.5", "localhost", "username", "password");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  @Path("/recent")
  @GET
  @Produces({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON})
  public String recent(@QueryParam("num") @DefaultValue("1") Integer numRecent) {
    List<TimedRegions> timings = tc.mostRecentTimings(numRecent);
    
    StringBuilder sb = new StringBuilder(256);
    sb.append("[");
    for (TimedRegions timing : timings) {
      sb.append("Timing:{").append(timing.toString()).append("}");
    }
    sb.append("]");
    
    return sb.toString();
  }

}
