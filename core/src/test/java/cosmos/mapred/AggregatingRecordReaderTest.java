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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import cosmos.mapred.AggregatingRecordReader;

public class AggregatingRecordReaderTest {  
  private static final String xml1 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + "<page>\n" + "  <a>A</a>\n" + "  <b>B</b>\n" + "</page>\n"
      + "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + "<page>\n" + "  <a>C</a>\n" + "  <b>D</b>\n" + "</page>\n" + "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
      + "<page>\n" + "  <a>E</a>\n" + "  <b>F</b>\n" + "</page>\n";
  
  private static final String xml2 = "  <b>B</b>\n" + "</page>\n" + "<page>\n" + "  <a>C</a>\n" + "  <b>D</b>\n" + "</page>\n" + "<page>\n" + "  <a>E</a>\n"
      + "  <b>F</b>\n" + "</page>\n";
  
  private static final String xml3 = "<page>\n" + "  <a>A</a>\n" + "  <b>B</b>\n" + "</page>\n" + "<page>\n" + "  <a>C</a>\n" + "  <b>D</b>\n" + "</page>\n"
      + "<page>\n" + "  <a>E</a>\n";
  
  private static final String xml4 = "<page>" + "  <a>A</a>" + "  <b>B</b>" + "</page>" + "<page>" + "  <a>C</a>" + "  <b>D</b>" + "</page>" + "<page>"
      + "  <a>E</a>" + "  <b>F</b>" + "</page>";
  
  
  private Configuration conf = null;
  private TaskAttemptContext ctx;
  private static DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    ctx = new TaskAttemptContext(conf, new TaskAttemptID());
  }
  
  public File createFile(String data) throws Exception {
    // Write out test file
    File f = File.createTempFile("aggReaderTest", ".xml");
    f.deleteOnExit();
    FileWriter writer = new FileWriter(f);
    writer.write(data);
    writer.flush();
    writer.close();
    return f;
  }
  
  private void testXML(Text xml, String aValue, String bValue, String attrValue) throws Exception {
    StringReader reader = new StringReader(xml.toString());
    InputSource source = new InputSource(reader);
    
    DocumentBuilder parser = factory.newDocumentBuilder();
    //parser.setErrorHandler(new MyErrorHandler());
    Document root = parser.parse(source);
    assertNotNull(root);
    
    reader = new StringReader(xml.toString());
    source = new InputSource(reader);
    
    reader = new StringReader(xml.toString());
    source = new InputSource(reader);
    
    reader = new StringReader(xml.toString());
    source = new InputSource(reader);
  }
  
  @Test
  public void testCorrectXML() throws Exception {
    File f = createFile(xml1);
    
    // Create FileSplit
    Path p = new Path(f.toURI().toString());
    FileSplit split = new FileSplit(p, 0, f.length(), null);
    
    // Initialize the RecordReader
    AggregatingRecordReader reader = new AggregatingRecordReader();
    reader.initialize(split, ctx);
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "A", "B", "");
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "C", "D", "");
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "E", "F", "");
    assertTrue(!reader.nextKeyValue());
    
  }
  
  @Test
  public void testPartialXML() throws Exception {
    File f = createFile(xml2);
    
    // Create FileSplit
    Path p = new Path(f.toURI().toString());
    FileSplit split = new FileSplit(p, 0, f.length(), null);
    
    // Initialize the RecordReader
    AggregatingRecordReader reader = new AggregatingRecordReader();
    reader.initialize(split, ctx);
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "C", "D", "");
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "E", "F", "");
    assertTrue(!reader.nextKeyValue());
  }
  
  public void testPartialXML2WithNoPartialRecordsReturned() throws Exception {
    File f = createFile(xml3);
    
    // Create FileSplit
    Path p = new Path(f.toURI().toString());
    FileSplit split = new FileSplit(p, 0, f.length(), null);
    
    // Initialize the RecordReader
    AggregatingRecordReader reader = new AggregatingRecordReader();
    reader.initialize(split, ctx);
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "A", "B", "");
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "C", "D", "");
    assertTrue(!reader.nextKeyValue());
  }
  
  @Test
  public void testPartialXML2() throws Exception {
    File f = createFile(xml3);
    
    // Create FileSplit
    Path p = new Path(f.toURI().toString());
    FileSplit split = new FileSplit(p, 0, f.length(), null);
    
    // Initialize the RecordReader
    AggregatingRecordReader reader = new AggregatingRecordReader();
    reader.initialize(split, ctx);
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "A", "B", "");
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "C", "D", "");
    assertFalse(reader.nextKeyValue());
  }
  
  @Test
  public void testLineSplitting() throws Exception {
    File f = createFile(xml4);
    
    // Create FileSplit
    Path p = new Path(f.toURI().toString());
    FileSplit split = new FileSplit(p, 0, f.length(), null);
    
    // Initialize the RecordReader
    AggregatingRecordReader reader = new AggregatingRecordReader();
    reader.initialize(split, ctx);
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "A", "B", "");
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "C", "D", "");
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "E", "F", "");
    assertTrue(!reader.nextKeyValue());
  }
  
  @Test
  public void testShortSplit() throws Exception {
    File f = createFile(xml3);
    
    // Create FileSplit
    Path p = new Path(f.toURI().toString());
    FileSplit split = new FileSplit(p, 0, 10, null);
    
    // Initialize the RecordReader
    AggregatingRecordReader reader = new AggregatingRecordReader();
    reader.initialize(split, ctx);
    assertTrue(reader.nextKeyValue());
    testXML(reader.getCurrentValue(), "A", "B", "");
    assertFalse(reader.nextKeyValue());
  }
  
}
