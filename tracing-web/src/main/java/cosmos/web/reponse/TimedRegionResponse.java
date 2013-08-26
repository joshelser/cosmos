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

import javax.xml.bind.annotation.XmlRootElement;

import cosmos.trace.Timings.TimedRegions.TimedRegion;

/**
 * 
 */
@XmlRootElement
public class TimedRegionResponse {
  
  protected String description;
  protected Long duration;
  
  public TimedRegionResponse() {}
  
  public TimedRegionResponse(TimedRegion region) {
    description = region.getDescription();
    duration = region.getDuration();
  }
  
  public TimedRegionResponse(String description, Long duration) {
    this.description = description;
    this.duration = duration;
  }
  
  /**
   * @return the description
   */
  public String getDescription() {
    return description;
  }
  
  /**
   * @param description
   *          the description to set
   */
  public void setDescription(String description) {
    this.description = description;
  }
  
  /**
   * @return the duration
   */
  public Long getDuration() {
    return duration;
  }
  
  /**
   * @param duration
   *          the duration to set
   */
  public void setDuration(Long duration) {
    this.duration = duration;
  }
  
  @Override
  public int hashCode() {
    return this.description.hashCode() ^ this.duration.hashCode();
  }
  
  @Override
  public boolean equals(Object o) {
    if (null == o) {
      return false;
    }
    
    if (o instanceof TimedRegionResponse) {
      TimedRegionResponse other = (TimedRegionResponse) o;
      
      return other.getDuration().equals(this.getDuration()) && other.getDescription().equals(this.getDescription());
    }
    
    return false;
  }
  
  @Override
  public String toString() {
    return "Description: " + description + ", Duration" + duration;
  }
  
}
