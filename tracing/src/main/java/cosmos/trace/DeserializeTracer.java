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
package cosmos.trace;

import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

import cosmos.trace.Timings.TimedRegions;

/**
 * 
 */
public class DeserializeTracer implements Function<Entry<Key,Value>,Tracer> {
  
  private static final Logger log = LoggerFactory.getLogger(DeserializeTracer.class);
  private static final DeserializeTracer INSTANCE = new DeserializeTracer();
  
  @Override
  public Tracer apply(Entry<Key,Value> entry) {
    return new Tracer(entry);
  }
  
  public static Tracer deserialize(Entry<Key,Value> entry) {
    return INSTANCE.apply(entry);
  }
}
