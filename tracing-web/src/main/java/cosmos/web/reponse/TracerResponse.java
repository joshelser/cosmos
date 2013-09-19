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
package cosmos.web.reponse;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import com.google.common.collect.Lists;

import cosmos.trace.Timings.TimedRegions.TimedRegion;
import cosmos.trace.Tracer;

/**
 * 
 */
@XmlRootElement
public class TracerResponse {
  
  protected List<TimedRegionResponse> regionTimings;
  protected String uuid;
  protected Long begin;
  
  public TracerResponse() {}
  
  public TracerResponse(Tracer tracer) {
    List<TimedRegion> regions = tracer.getTimings();
    regionTimings = Lists.newArrayListWithExpectedSize(regions.size());
    uuid = tracer.getUUID();
    begin = tracer.getBegin();
    
    for (TimedRegion region : regions) {
      regionTimings.add(new TimedRegionResponse(region));
    }
  }

  /**
   * @return the regionTimings
   */
  public List<TimedRegionResponse> getRegionTimings() {
    return regionTimings;
  }

  /**
   * @param regionTimings the regionTimings to set
   */
  public void setRegionTimings(List<TimedRegionResponse> regionTimings) {
    this.regionTimings = regionTimings;
  }
  
  /**
   * @return the uuid
   */
  public String getUuid() {
    return uuid;
  }

  /**
   * @param uuid the uuid to set
   */
  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  /**
   * @return the begin
   */
  public Long getBegin() {
    return begin;
  }

  /**
   * @param begin the begin to set
   */
  public void setBegin(Long begin) {
    this.begin = begin;
  }

  @Override
  public String toString() {
    return uuid + ", " + begin + ", " + regionTimings.toString();
  }
  
}
