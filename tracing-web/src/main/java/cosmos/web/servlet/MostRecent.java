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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import cosmos.trace.Tracer;
import cosmos.trace.TracerClient;
import cosmos.web.CosmosWebConfiguration;
import cosmos.web.reponse.TracerResponse;

/**
 * 
 */
@Path("/cosmos")
@Produces({MediaType.APPLICATION_JSON})
public class MostRecent {
  private static final Logger log = LoggerFactory.getLogger(MostRecent.class);

  protected final TracerClient tc;
  protected final SimpleDateFormat yyyymmddFormat = new SimpleDateFormat("yyyyMMdd");

  public MostRecent() {
    Configuration conf = CosmosWebConfiguration.get();

    try {
      tc = new TracerClient(conf.get(CosmosWebConfiguration.ACCUMULO_INSTANCE), conf.get(CosmosWebConfiguration.ZOOKEEPERS),
          conf.get(CosmosWebConfiguration.ACCUMULO_USER), conf.get(CosmosWebConfiguration.ACCUMULO_PASSWORD));
    } catch (Exception e) {
      log.error("Could not create tracer client", e);
      throw new RuntimeException(e);
    }
  }

  protected synchronized Date parseDate(String input) {
    try {
      return yyyymmddFormat.parse(input);
    } catch (ParseException e) {
      throw new RuntimeException("Input '" + input + "' must be a date in the form: yyyyMMdd", e);
    }
  }

  protected TracerResponse transform(Tracer t) {
    return new TracerResponse(t);
  }

  protected List<TracerResponse> transform(List<Tracer> tracers) {
    List<TracerResponse> tracerResponses = Lists.newArrayListWithExpectedSize(tracers.size());

    for (Tracer tracer : tracers) {
      tracerResponses.add(transform(tracer));
    }

    return tracerResponses;
  }

  @Path("/recent")
  @GET
  public List<TracerResponse> recent(@QueryParam("num") @DefaultValue("10") Integer numRecent) {
    List<Tracer> timings = tc.mostRecentTimings(numRecent);

    return transform(timings);
  }

  @Path("/since")
  @GET
  public List<TracerResponse> between(@QueryParam("start") String start) {
    List<Tracer> timings = Lists.newArrayList(tc.since(parseDate(start)));

    return transform(timings);
  }

  @Path("/between")
  @GET
  public List<TracerResponse> between(@QueryParam("start") String start, @QueryParam("end") String end) {
    List<Tracer> timings = Lists.newArrayList(tc.between(parseDate(start), parseDate(end)));

    return transform(timings);
  }

  @Path("/timing")
  @GET
  public TracerResponse timing(@QueryParam("uuid") String uuid) {
    Tracer timing = null;
    try {
      timing = tc.getTimings(uuid);
    } catch (NoSuchElementException e) {
      throw new WebApplicationException(404);
    }

    return transform(timing);
  }
}
