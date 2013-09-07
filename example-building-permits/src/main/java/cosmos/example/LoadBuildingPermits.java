package cosmos.example;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;

import cosmos.Cosmos;
import cosmos.impl.SortableResult;
import cosmos.options.Defaults;
import cosmos.results.Column;
import cosmos.results.SValue;
import cosmos.results.impl.MultimapQueryResult;

public class LoadBuildingPermits implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(LoadBuildingPermits.class);
  
  public static final Column ID = Column.create("ID");
  public static final Long MAX_RESULTS = Long.MAX_VALUE;
  
  protected Cosmos cosmos;
  protected SortableResult id;
  protected File csvData;
  protected Long maxResultsToLoad;
  
  public LoadBuildingPermits(Cosmos cosmos, SortableResult id, File csvData) {
    checkNotNull(cosmos);
    checkNotNull(id);
    checkNotNull(csvData);
    checkArgument(csvData.exists() && csvData.isFile() && csvData.canRead(), 
        csvData.getAbsoluteFile() + " is not a readable file");
    
    this.cosmos = cosmos;
    this.id = id;
    this.csvData = csvData;
    
    // Control how many results from the data set are loaded
    this.maxResultsToLoad = MAX_RESULTS;
    // this.maxResultsToLoad = 50000l;
  }
  
  @Override
  public void run() {
    ArrayList<String> schema;
    
    // Open up a regular reader to get the header line out of the file, 
    // and construct a schema for this csv file.
    try {
      BufferedReader bufReader = new BufferedReader(new FileReader(csvData));
      String schemaLine = bufReader.readLine();
      
      String[] schemaEntries = StringUtils.split(schemaLine, Defaults.COMMA);
      schema = Lists.newArrayListWithCapacity(schemaEntries.length);
      for (String schemaEntry : schemaEntries) {
        schema.add(StringUtils.trim(schemaEntry));
      }
      
      bufReader.close();
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
      
    try {
      CSVReader reader = new CSVReader(new FileReader(csvData));
      
      // Consume the schema line as we don't want to treat it as a record
      reader.readNext();
        
      String[] data;
      long lineNumber = 0l;
      
      long resultsInserted = 0l;
      int itemsToBuffer = 1000;
      
      // Buffer 100 results to ammortize the BatchWriter costs underneath
      ArrayList<MultimapQueryResult> cachedResults = Lists.newArrayListWithCapacity(itemsToBuffer + 1);
      while (null != (data = reader.readNext())) {
        lineNumber++;
        
        // Make sure our line of data has the expected number of columns
        if (data.length != schema.size()) {
          log.error("Data on line {} did not match expected column size: {}", lineNumber, data);
          continue;
        }
        
        // Make the Multimap of column to svalue
        HashMultimap<Column,SValue> metadata = HashMultimap.create();
        for (int i = 0; i < schema.size(); i++) {
          String name = schema.get(i);
          String value = StringUtils.trim(data[i]);
          
          if (!StringUtils.isBlank(value)) {
            metadata.put(Column.create(name), SValue.create(value, Defaults.EMPTY_VIS));
          }
        }
        
        // Make sure we can extract a column as the docID
        if (!metadata.containsKey(ID) || 1 != metadata.get(ID).size()) {
          log.error("Expected to find one {} column in record: {}", ID, metadata.toString());
        }
        
        SValue docId = metadata.get(ID).iterator().next();
        
        // Add the record to our buffer
        cachedResults.add(new MultimapQueryResult(metadata, docId.value(), Defaults.EMPTY_VIS));
        
        // Flush the buffer when it gets big enough
        if (itemsToBuffer < cachedResults.size()) {
          try {
            cosmos.addResults(this.id, cachedResults);
          } catch (Exception e) {
            log.error("Problem adding results to cosmos", e);
            throw new RuntimeException(e);
          }
          
          resultsInserted += cachedResults.size();
          
          cachedResults.clear();
        }
        
        // Observe a limit on how many results we'll load after each batch
        if (resultsInserted >= this.maxResultsToLoad) {
          break;
        }
      }
      
      // Make sure we catch the tail-end of any results we buffered
      if (!cachedResults.isEmpty()) {
        try {
          cosmos.addResults(this.id, cachedResults);
        } catch (Exception e) {
          log.error("Problem adding results to cosmos", e);
          throw new RuntimeException(e);
        }
      }
      
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
