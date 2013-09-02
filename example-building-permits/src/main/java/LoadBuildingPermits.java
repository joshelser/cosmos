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

import cosmos.impl.SortableResult;
import cosmos.options.Defaults;

public class LoadBuildingPermits implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(LoadBuildingPermits.class);
  
  protected SortableResult id;
  protected File csvData;
  
  public LoadBuildingPermits(SortableResult id, File csvData) {
    checkNotNull(id);
    checkNotNull(csvData);
    checkArgument(csvData.exists() && csvData.isFile() && csvData.canRead(), 
        csvData.getAbsoluteFile() + " is not a readable file");
    
    this.id = id;
    this.csvData = csvData;
  }
  
  @Override
  public void run() {
    ArrayList<String> schema;
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
      
      // Consume the schema line
      reader.readNext();
        
      String[] data;
      long lineNumber = 0l;
      while (null != (data = reader.readNext())) {
        lineNumber++;
        
        if (data.length != schema.size()) {
          log.error("Data on line {} did not match expected column size: {}", lineNumber, data);
          continue;
        }
        
        HashMultimap<String,String> metadata = HashMultimap.create();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    

  }
}
