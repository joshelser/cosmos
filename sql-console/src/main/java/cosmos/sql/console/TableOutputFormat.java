package cosmos.sql.console;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

import cosmos.records.RecordValue;
import cosmos.results.Column;

public class TableOutputFormat {
  public static final String COLUMN_SEPARATOR = "|", CORNER = "+", SPACE = " ", HYPHEN = "-", NEWLINE = "\n";
  
  public void print(ResultSet results, Stopwatch timer) throws SQLException {
    final ResultSetMetaData metadata = results.getMetaData();
    final int columnCount = metadata.getColumnCount();
    List<String> columns = Lists.newArrayListWithExpectedSize(columnCount);
    List<Integer> columnWidths = Lists.newArrayListWithExpectedSize(columnCount);
    
    for (int i = 1; i <= columnCount; i++) {
      String columnName = metadata.getColumnName(i);
      columns.add(columnName);
      // Account for padding
      columnWidths.add(Math.max(8, columnName.length()));
    }
    
    final StringBuilder line = new StringBuilder(1024);
    final StringBuilder sb = new StringBuilder(256);
    long linesWritten = 0;
    while (results.next()) {
      for (int i = 0; i < columns.size(); i++) {
        String column = columns.get(i);
        List<Entry<Column,RecordValue>> sValues = (List<Entry<Column,RecordValue>>) results.getObject(column);
        
        if (null != sValues && !sValues.isEmpty()) {
          for (Entry<Column,RecordValue> values : sValues) {
            sb.append(values.getValue().toString());
          }
        }
        
        if (columnWidths.get(i) < sb.length()) {
          columnWidths.set(i, sb.length());
        } else {
          padColumnValueToWidth(sb, columnWidths.get(i));
        }
        
        if (0 == i) {
          line.append(COLUMN_SEPARATOR);
        }
        
        line.append(padColumnValue(sb)).append(COLUMN_SEPARATOR);
        sb.setLength(0);
      }
      
      if (0 == linesWritten) {
        for (Integer columnWidth : columnWidths) {
          sb.append(CORNER);
          // +2 to account for the space we place around the "fixed" width of the column
          // done in {@link #padColumnValue(StringBuilder)}
          for (int i = 0; i < columnWidth + 2; i++) {
            sb.append(HYPHEN);
          }
        }
        
        sb.append(CORNER).append(NEWLINE);
        sb.append(COLUMN_SEPARATOR);
        
        for (int i = 0; i < columns.size(); i++) {
          String column = columns.get(i);
          StringBuilder columnHeader = padColumnValueToWidth(column, columnWidths.get(i));
          padColumnValue(columnHeader).append(COLUMN_SEPARATOR);
          sb.append(columnHeader);
        }
        
        sb.append(NEWLINE);

        for (Integer columnWidth : columnWidths) {
          sb.append(CORNER);
          // +2 to account for the space we place around the "fixed" width of the column
          // done in {@link #padColumnValue(StringBuilder)}
          for (int i = 0; i < columnWidth + 2; i++) {
            sb.append(HYPHEN);
          }
        }
        
        sb.append(CORNER).append(NEWLINE);
        
        line.insert(0, sb.toString());
        sb.setLength(0);
      }

      System.out.println(line);
      line.setLength(0);
      linesWritten++;
    }
    
    timer.stop();

    for (Integer columnWidth : columnWidths) {
      line.append(CORNER);
      // +2 to account for the space we place around the "fixed" width of the column
      // done in {@link #padColumnValue(StringBuilder)}
      for (int i = 0; i < columnWidth + 2; i++) {
        line.append(HYPHEN);
      }
    }
    
    line.append(CORNER);
    
    System.out.println(line);
    System.out.println(linesWritten + " rows in set (" + timer + ")\n");
    
    return;
  }
  
  protected StringBuilder padColumnValue(StringBuilder columnValue) {
    return columnValue.append(SPACE).insert(0, SPACE);
  }
  
  protected StringBuilder padColumnValueToWidth(String columnValue, int width) {
    return padColumnValueToWidth(new StringBuilder(columnValue), width);
  }
  
  protected StringBuilder padColumnValueToWidth(StringBuilder columnValue, int width) {
    int charsToAdd = width - columnValue.length();
    for (int i = 0; i < charsToAdd; i++) {
      columnValue.append(SPACE);
    }
    return columnValue;
  }
}
