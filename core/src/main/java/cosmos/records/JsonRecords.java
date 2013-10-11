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
package cosmos.records;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import cosmos.options.Defaults;
import cosmos.records.docids.CountingDocIdGenerator;
import cosmos.records.docids.DocIdGenerator;
import cosmos.records.functions.StringToStringRecordFunction;
import cosmos.records.impl.MapRecord;

/**
 * 
 */
public class JsonRecords {
  private static final Logger log = LoggerFactory.getLogger(JsonRecords.class);
  
  protected static final Type listOfMapsType = new TypeToken<List<Map<String,String>>>(){}.getType();
  
  /**
   * Takes a JSON file and creates {@link MapRecord}s from the contents. Monotonically increasing docIds are applied
   * to each {@link Record} parsed from the JSON.
   * @param jsonFile A local file of JSON. Must be a List of Maps
   * @return A List of {@link MapRecord}s 
   * @throws FileNotFoundException
   * @throws IOException
   * @throws JsonSyntaxException
   */
  public static List<MapRecord> fromJson(File jsonFile) throws FileNotFoundException, IOException, JsonSyntaxException {
    return fromJson(jsonFile, new CountingDocIdGenerator());
  }
  
  /**
   * Takes a JSON file and creates {@link MapRecord}s from the contents. Uses the provided {@link DocIdGenerator} to 
   * generate the docId when creating the {@link MapRecord}
   * @param jsonFile
   * @param generator
   * @return
   * @throws FileNotFoundException
   * @throws IOException
   * @throws JsonSyntaxException
   */
  public static List<MapRecord> fromJson(File jsonFile, DocIdGenerator generator) throws FileNotFoundException, IOException, JsonSyntaxException {
    Preconditions.checkNotNull(jsonFile);
    Preconditions.checkArgument(jsonFile.exists() && jsonFile.isFile() && jsonFile.canRead(), jsonFile + " is not a readable file");
    Preconditions.checkNotNull(generator);
    
    final StringBuilder sb = new StringBuilder(128);
    FileReader fileReader = new FileReader(jsonFile);
    BufferedReader bufReader = new BufferedReader(fileReader);
    String line;
    while ((line = bufReader.readLine()) != null) {
      sb.append(line);
    }
    
    bufReader.close();
    
    String json = sb.toString();
    
    return fromJson(json, generator);
  }
  
  public static List<MapRecord> fromJson(String json) throws JsonSyntaxException {
    return fromJson(json, new CountingDocIdGenerator());
  }
  
  public static List<MapRecord> fromJson(String json, DocIdGenerator generator) throws JsonSyntaxException {
    Preconditions.checkNotNull(json);
    Preconditions.checkNotNull(generator);
    
    final Gson gson = new Gson();
    List<Map<String,String>> maps = null;
    try {
      maps = gson.fromJson(json, listOfMapsType);
    } catch (JsonSyntaxException e) {
      log.error("Expected JSON to be a List of Maps");
      throw e;
    }
    
    if (null == maps) {
      return Collections.emptyList();
    }
    
    List<MapRecord> records = Lists.newArrayListWithExpectedSize(maps.size());
    for (Map<String,String> map : maps) {
      records.add(new MapRecord(map, generator.getDocId(map), Defaults.EMPTY_VIS, new StringToStringRecordFunction()));
    }
    
    return records;
  }
}
