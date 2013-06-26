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
package sorts.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;

/**
 * A copy of {@link LineRecordReader} which does not discard lines longer than "mapred.linerecordreader.maxlength". Instead, it returns them, leaving it to the
 * mapper to decide what to do with it. It also does not treat '\r' (CR) characters as new lines -- it uses {@link LfLineReader} instead of {@link LineReader}
 * to read lines.
 */
public class LongLineRecordReader extends RecordReader<LongWritable,Text> {
  protected CompressionCodecFactory compressionCodecs = null;
  protected long start;
  protected long pos;
  protected long end;
  protected LfLineReader in;
  protected int maxLineLength;
  protected LongWritable key = null;
  protected Text value = null;
  
  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    compressionCodecs = new CompressionCodecFactory(job);
    final CompressionCodec codec = compressionCodecs.getCodec(file);
    
    // open the file and seek to the start of the split
    FileSystem fs = file.getFileSystem(job);
    FSDataInputStream fileIn = fs.open(split.getPath());
    boolean skipFirstLine = false;
    if (codec != null) {
      in = new LfLineReader(codec.createInputStream(fileIn), job);
      end = Long.MAX_VALUE;
    } else {
      if (start != 0) {
        skipFirstLine = true;
        --start;
        fileIn.seek(start);
      }
      in = new LfLineReader(fileIn, job);
    }
    if (skipFirstLine) { // skip first line and re-establish "start".
      start += in.readLine(new Text(), 0, (int) Math.min(Integer.MAX_VALUE, end - start));
    }
    this.pos = start;
  }
  
  @Override
  public boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new LongWritable();
    }
    key.set(pos);
    if (value == null) {
      value = new Text();
    }
    int newSize = 0;
    if (pos < end) {
      newSize = in.readLine(value, maxLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
      if (newSize != 0) {
        pos += newSize;
      }
    }
    if (newSize == 0) {
      key = null;
      value = null;
      return false;
    } else {
      return true;
    }
  }
  
  @Override
  public LongWritable getCurrentKey() {
    return key;
  }
  
  @Override
  public Text getCurrentValue() {
    return value;
  }
  
  /**
   * Get the progress within the split
   */
  @Override
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float) (end - start));
    }
  }
  
  @Override
  public synchronized void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }
}
