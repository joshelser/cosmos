package cosmos.sql.console;

import org.junit.Assert;
import org.junit.Test;

public class TableOutputFormatTest {
  
  @Test
  public void test() {
    TableOutputFormat tableFormat = new TableOutputFormat();
    
    Assert.assertEquals("FOO   ", tableFormat.padColumnValueToWidth("FOO", 6).toString());
  }
  
}
