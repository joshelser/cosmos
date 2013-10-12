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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import cosmos.options.Defaults;
import cosmos.records.docids.CountingDocIdGenerator;
import cosmos.records.docids.DocIdGenerator;
import cosmos.records.impl.MapRecord;
import cosmos.records.impl.MultimapRecord;
import cosmos.records.values.BooleanRecordValue;
import cosmos.records.values.DoubleRecordValue;
import cosmos.records.values.IntegerRecordValue;
import cosmos.records.values.LongRecordValue;
import cosmos.records.values.NumberRecordValue;
import cosmos.records.values.RecordValue;
import cosmos.records.values.StringRecordValue;
import cosmos.results.Column;

/**
 * 
 */
public class JsonRecords {
  private static final Logger log = LoggerFactory.getLogger(JsonRecords.class);
  
  protected static final Type listOfMapsType = new TypeToken<List<Map<String,RecordValue<?>>>>(){}.getType();
  
  
  /**
   * Takes a JSON file and creates {@link MapRecord}s from the contents. Monotonically increasing docIds are applied
   * to each {@link Record} parsed from the JSON.
   * @param jsonFile A local file of JSON. Must be an array of objects.
   * @return A List of {@link MapRecord}s 
   * @throws FileNotFoundException
   * @throws IOException
   * @throws JsonSyntaxException
   */
  public static List<MapRecord> fromJsonAsMap(File jsonFile) throws FileNotFoundException, IOException, JsonSyntaxException {
    return fromJsonAsMap(jsonFile, new CountingDocIdGenerator());
  }

  /**
   * Takes a JSON file and creates {@link MultimapRecord}s from the contents. Monotonically increasingt docIds are applied
   * to each {@link Record} parsed from the JSON.
   * @param jsonFile A local file of JSON. Must be an array of objects.
   * @return A List of {@link MultimapRecord}s
   * @throws FileNotFoundException
   * @throws IOException
   * @throws JsonSyntaxException
   */
  public static List<MultimapRecord> fromJsonAsMultiMap(File jsonFile) throws FileNotFoundException, IOException, JsonSyntaxException {
    return fromJsonAsMultimap(jsonFile, new CountingDocIdGenerator());
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
  public static List<MapRecord> fromJsonAsMap(File jsonFile, DocIdGenerator generator) throws FileNotFoundException, IOException, JsonSyntaxException {
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
    
    return fromJsonAsMap(json, generator);
  }
  public static List<MultimapRecord> fromJsonAsMultimap(File jsonFile, DocIdGenerator generator) throws FileNotFoundException, IOException, JsonSyntaxException {
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
    
    return fromJsonAsMultimap(json, generator);
  }
  
  public static List<MapRecord> fromJsonAsMap(String json) throws JsonSyntaxException {
    return fromJsonAsMap(json, new CountingDocIdGenerator());
  }
  
  public static List<MultimapRecord> fromJsonAsMultimap(String json) throws JsonSyntaxException {
    return fromJsonAsMultimap(json, new CountingDocIdGenerator());
  }
  
  public static List<MapRecord> fromJsonAsMap(String json, DocIdGenerator generator) throws JsonSyntaxException {
    Preconditions.checkNotNull(json);
    Preconditions.checkNotNull(generator);
    
    JsonParser parser = new JsonParser();
    JsonElement topLevelElement = parser.parse(json);
    
    if (topLevelElement.isJsonNull()) {
      return Collections.emptyList();
    } else if (!topLevelElement.isJsonArray()) {
      throw new JsonSyntaxException("Expected a list of dictionaries");
    }
    
    final LinkedList<MapRecord> records = Lists.newLinkedList();
    
    // Walk the top level list
    JsonArray list = topLevelElement.getAsJsonArray();
    for (JsonElement e : list) {
      // Make sure that it's a Json Object
      if (!e.isJsonObject()) {
        throw new JsonSyntaxException("Expected a Json Object");
      }
      
      // Parse each "Object" (map)
      JsonObject map = e.getAsJsonObject();
      
      records.add(asMapRecord(map, generator));
    }
    
    return records;
  }
  
  public static List<MultimapRecord> fromJsonAsMultimap(String json, DocIdGenerator generator) throws JsonSyntaxException {
    Preconditions.checkNotNull(json);
    Preconditions.checkNotNull(generator);
    
    JsonParser parser = new JsonParser();
    JsonElement topLevelElement = parser.parse(json);
    
    if (topLevelElement.isJsonNull()) {
      return Collections.emptyList();
    } else if (!topLevelElement.isJsonArray()) {
      throw new JsonSyntaxException("Expected a list of dictionaries");
    }
    
    final LinkedList<MultimapRecord> records = Lists.newLinkedList();
    
    // Walk the top level list
    JsonArray list = topLevelElement.getAsJsonArray();
    for (JsonElement e : list) {
      // Make sure that it's a Json Object
      if (!e.isJsonObject()) {
        throw new JsonSyntaxException("Expected a Json Object");
      }
      
      // Parse each "Object" (map)
      JsonObject map = e.getAsJsonObject();
      
      records.add(asMultimapRecord(map, generator));
    }
    
    return records;
  }
  
  /**
   * Parses a {@link JsonObject{ into a MapRecord. A duplicate key in the JsonObject will
   * overwrite the previous key.
   * @param map The JsonObject being parsed
   * @param generator Construct to generate a docId for this {@link MapRecord}
   * @return A MapRecord built from the {@link JsonObject}
   */
  protected static MapRecord asMapRecord(JsonObject map, DocIdGenerator generator) {
    Map<Column,RecordValue<?>> data = Maps.newHashMap();
    for (Entry<String,JsonElement> entry : map.entrySet()) {
      final Column key = Column.create(entry.getKey());
      final JsonElement value = entry.getValue();
      
      if (value.isJsonNull()) {
        data.put(key, null);
      } else if (value.isJsonPrimitive()) {
        JsonPrimitive primitive = (JsonPrimitive) value;
        
        // Numbers
        if (primitive.isNumber()) {
          NumberRecordValue<?> v;
          
          double d = primitive.getAsDouble();
          if ((int) d == d) {
            v = new IntegerRecordValue((int) d, Defaults.EMPTY_VIS);
          } else if ((long) d == d) {
            v = new LongRecordValue((long) d, Defaults.EMPTY_VIS);
          } else {
            v = new DoubleRecordValue(d, Defaults.EMPTY_VIS);
          }
          
          data.put(key, v);
          
        } else if (primitive.isString()) {
          // String
          data.put(key, new StringRecordValue(primitive.getAsString(), Defaults.EMPTY_VIS));
          
        } else if (primitive.isBoolean()) {
          // Boolean
          data.put(key, new BooleanRecordValue(primitive.getAsBoolean(), Defaults.EMPTY_VIS));
          
        } else if (primitive.isJsonNull()) {
          // Is this redundant?
          data.put(key, null);
        } else {
          throw new RuntimeException("Unhandled primitive: " + primitive);
        }
      } else {
        throw new RuntimeException("Expected a String, Number or Boolean");
      }
    }
    
    return new MapRecord(data, generator.getDocId(data), Defaults.EMPTY_VIS);
  }
  
  /**
   * Builds a {@link MultimapRecord} from the provided {@link JsonObject} using a docid
   * from the {@link DocIdGenerator}. This method will support lists of values for a key
   * in the {@link JsonObject} but will fail on values which are {@link JsonObject}s and 
   * values which have nested {@link JsonArray}s. 
   * @param map The {@link JsonObject} to build this {@link MultimapRecord} from
   * @param generator {@link DocIdGenerator} to construct a docid for this {@link MultimapRecord}
   * @return A {@link MultimapRecord} built from the provided arguments.
   */
  protected static MultimapRecord asMultimapRecord(JsonObject map, DocIdGenerator generator) {
    Multimap<Column,RecordValue<?>> data = HashMultimap.create();
    for (Entry<String,JsonElement> entry : map.entrySet()) {
      final Column key = Column.create(entry.getKey());
      final JsonElement value = entry.getValue();
      
      if (value.isJsonNull()) {
        data.put(key, null);
      } else if (value.isJsonPrimitive()) {
        JsonPrimitive primitive = (JsonPrimitive) value;
        
        // Numbers
        if (primitive.isNumber()) {
          NumberRecordValue<?> v;
          
          double d = primitive.getAsDouble();
          if ((int) d == d) {
            v = new IntegerRecordValue((int) d, Defaults.EMPTY_VIS);
          } else if ((long) d == d) {
            v = new LongRecordValue((long) d, Defaults.EMPTY_VIS);
          } else {
            v = new DoubleRecordValue(d, Defaults.EMPTY_VIS);
          }
          
          data.put(key, v);
          
        } else if (primitive.isString()) {
          // String
          data.put(key, new StringRecordValue(primitive.getAsString(), Defaults.EMPTY_VIS));
          
        } else if (primitive.isBoolean()) {
          // Boolean
          data.put(key, new BooleanRecordValue(primitive.getAsBoolean(), Defaults.EMPTY_VIS));
          
        } else if (primitive.isJsonNull()) {
          // Is this redundant?
          data.put(key, null);
        } else {
          throw new RuntimeException("Unhandled primitive: " + primitive);
        }
      } else if (value.isJsonArray()) {
        
        // Multimaps should handle the multiple values, not fail
        JsonArray values = value.getAsJsonArray();
        for (JsonElement element : values) {
          if (element.isJsonNull()) {
            data.put(key, null);
          } else if (element.isJsonPrimitive()) {

            JsonPrimitive primitive = (JsonPrimitive) element;
            
            // Numbers
            if (primitive.isNumber()) {
              NumberRecordValue<?> v;
              
              double d = primitive.getAsDouble();
              if ((int) d == d) {
                v = new IntegerRecordValue((int) d, Defaults.EMPTY_VIS);
              } else if ((long) d == d) {
                v = new LongRecordValue((long) d, Defaults.EMPTY_VIS);
              } else {
                v = new DoubleRecordValue(d, Defaults.EMPTY_VIS);
              }
              
              data.put(key, v);
              
            } else if (primitive.isString()) {
              // String
              data.put(key, new StringRecordValue(primitive.getAsString(), Defaults.EMPTY_VIS));
              
            } else if (primitive.isBoolean()) {
              // Boolean
              data.put(key, new BooleanRecordValue(primitive.getAsBoolean(), Defaults.EMPTY_VIS));
              
            } else if (primitive.isJsonNull()) {
              // Is this redundant?
              data.put(key, null);
            } else {
              throw new RuntimeException("Unhandled Json primitive: " + primitive);
            }
          } else {
            throw new RuntimeException("Expected a Json primitive");
          }
        }
        
      } else {
        throw new RuntimeException("Expected a String, Number or Boolean");
      }
    }
    
    return new MultimapRecord(data, generator.getDocId(data), Defaults.EMPTY_VIS);

  }
}
