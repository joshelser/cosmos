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
package cosmos.mapred;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


/**
 * This class aggregates Text values based on a start and end filter. An example use case for this would be XML data. This will not work with data that has
 * nested start and stop tokens.
 * 
 */
public class AggregatingRecordReader extends LongLineRecordReader {
  
  private LongWritable key = new LongWritable();
  private String startToken = null;
  private String endToken = null;
  private long counter = 0;
  private Text aggValue = new Text();
  private boolean startFound = false;
  private StringBuilder remainder = new StringBuilder(4096);
  private boolean returnPartialMatches = false;
  
  @Override
  public LongWritable getCurrentKey() {
    key.set(counter);
    return key;
  }
  
  @Override
  public Text getCurrentValue() {
    return aggValue;
  }
  
  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
    super.initialize(genericSplit, context);
    this.startToken = "<page>";
    this.endToken = "</page>";
    
    /*
     * Text-appending works almost exactly like the + operator on Strings- it creates a byte array exactly the size of [prefix + suffix] and dumps the bytes
     * into the new array. This module works by doing lots of little additions, one line at a time. With most XML, the documents are partitioned on line
     * boundaries, so we will generally have lots of additions. Setting a large default byte array for a text object can avoid this and give us
     * StringBuilder-like functionality for Text objects.
     */
    byte[] txtBuffer = new byte[4096];
    aggValue.set(txtBuffer);
  }
  
  @Override
  public boolean nextKeyValue() throws IOException {
    aggValue.clear();
    
    boolean hasNext = false;
    boolean finished = false;
    // Find the start token
    while (!finished && (((hasNext = super.nextKeyValue()) == true) || remainder.length() > 0)) {
      if (hasNext)
        finished = process(super.getCurrentValue());
      else
        finished = process(null);
      if (finished) {
        startFound = false;
        counter++;
        return true;
      }
    }

    boolean endFound = false;
    
    // We found a start tag but no end tag
    if (aggValue.getLength() > 0 && startFound) {
      final Text buf = new Text();
      int bytesRead = 0;
      while (!endFound && (bytesRead = in.readLine(buf)) > 0) {
        if (process(buf)) {
          endFound = true;
        }
      }
    }
    
    if (endFound) {
      startFound = false;
      counter++;
      return true;
    }
    
    return false;
  }
  
  /**
   * Populates aggValue with the contents of the Text object.
   * 
   * @param t
   * @return true if aggValue is complete, else false and needs more data.
   */
  private boolean process(Text t) throws IOException {
    
    if (null != t) {
      remainder.append(t.toString());
    }
    
    while (remainder.length() > 0) {
      if (!startFound) {
        // If found, then begin aggregating at the start offset
        int start = remainder.indexOf(startToken);
        if (-1 != start) {
          // Append the start token to the aggregate value
          textAppend(aggValue, remainder.substring(start, start + startToken.length()));
          // Remove to the end of the start token from the remainder
          remainder.delete(0, start + startToken.length());
          startFound = true;
        } else {
          // If we are looking for the start and have not found it, then remove
          // the bytes
          remainder.delete(0, remainder.length());
        }
      } else {
        // Try to find the end
        int end = remainder.indexOf(endToken);
        // Also try to find the start
        int start = remainder.indexOf(startToken);
        if (-1 == end) {
          if (returnPartialMatches && start >= 0) {
            // End token not found, but another start token was found...
            // The amount to copy is up to the beginning of the next start token
            textAppend(aggValue, remainder.substring(0, start));
            remainder.delete(0, start);
            return true;
          } else {
            // Not found, aggregate the entire remainder
            textAppend(aggValue, remainder.toString());
            // Delete all chars from remainder
            remainder.delete(0, remainder.length());
          }
        } else {
          if (returnPartialMatches && start >= 0 && start < end) {
            // We found the end token, but found another start token first, so
            // deal with that.
            textAppend(aggValue, remainder.substring(0, start));
            remainder.delete(0, start);
            return true;
          } else {
            // END_TOKEN was found. Extract to the end of END_TOKEN
            textAppend(aggValue, remainder.substring(0, end + endToken.length()));
            // Remove from remainder up to the end of END_TOKEN
            remainder.delete(0, end + endToken.length());
            return true;
          }
        }
      }
    }
    return false;
  }
  
  private void textAppend(Text t, String s) throws IOException {
    try {
      ByteBuffer buf = Text.encode(s, false);
      t.append(buf.array(), 0, buf.limit());
    } catch (CharacterCodingException e) {
      throw new IOException(e);
    }
  }
  
}
