package sorts.results;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Assert;
import org.junit.Test;

import sorts.results.impl.MultimapQueryResult;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * 
 */
public class MultimapQueryResultTest extends AbstractSortableTest {
  
  @Test
  public void identityWritableEquality() throws Exception {
    Multimap<Column,SValue> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), SValue.create("foo", VIZ));
    data.put(Column.create("TEXT"), SValue.create("bar", VIZ));
    
    MultimapQueryResult mqr = new MultimapQueryResult(data, "1", VIZ);
    
    DataOutputBuffer out = new DataOutputBuffer();
    mqr.write(out);
    
    DataInputBuffer in = new DataInputBuffer();
    
    byte[] bytes = out.getData();
    in.reset(bytes, out.getLength());
    
    MultimapQueryResult mqr2 = MultimapQueryResult.recreate(in);
    
    Assert.assertEquals(mqr, mqr2);
  }
  
  @Test
  public void nonEqual() throws Exception {
    Multimap<Column,SValue> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), SValue.create("foo", VIZ));
    data.put(Column.create("TEXT"), SValue.create("bar", VIZ));
    
    MultimapQueryResult mqr = new MultimapQueryResult(data, "1", VIZ);
    MultimapQueryResult mqr2 = new MultimapQueryResult(data, "2", VIZ);
    
    Assert.assertNotEquals(mqr, mqr2);
    
    MultimapQueryResult mqr3 = new MultimapQueryResult(data, "1", new ColumnVisibility("foobarbarbarbarbarbar"));
    
    Assert.assertNotEquals(mqr, mqr3);
    Assert.assertNotEquals(mqr2, mqr3);
    
    data = HashMultimap.create(data);
    
    data.put(Column.create("FOO"), SValue.create("barfoo", VIZ));
    
    MultimapQueryResult mqr4 = new MultimapQueryResult(data, "1", VIZ);
    
    Assert.assertNotEquals(mqr, mqr4);
    Assert.assertNotEquals(mqr2, mqr4);
    Assert.assertNotEquals(mqr3, mqr4);
  }
  
}
