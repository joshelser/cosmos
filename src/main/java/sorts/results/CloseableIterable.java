package sorts.results;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

/**
 * 
 */
public class CloseableIterable<T> implements Results<T> {
  
  protected final ScannerBase scanner;
  protected final Iterable<T> iterable;
  
  public CloseableIterable(ScannerBase scanner, Iterable<T> iterable) {
    checkNotNull(scanner);
    checkNotNull(iterable);
    
    this.scanner = scanner;
    this.iterable = iterable;
  }
  
  public static <T> CloseableIterable<T> create(ScannerBase scanner, Iterable<T> iterable) {
    return new CloseableIterable<T>(scanner, iterable);
  }
  
  public static <T> CloseableIterable<T> transform(ScannerBase scanner, Function<Entry<Key,Value>,T> func) {
    return new CloseableIterable<T>(scanner, Iterables.transform(scanner, func));
  }
  
  public static <T> CloseableIterable<T> filterAndTransform(ScannerBase scanner, Predicate<Entry<Key,Value>> filter, Function<Entry<Key,Value>,T> func) {
    return new CloseableIterable<T>(scanner, Iterables.transform(Iterables.filter(scanner, filter), func));
  }
  
  protected ScannerBase source() {
    return scanner;
  }
  
  @Override
  public Iterator<T> iterator() {
    return iterable.iterator();
  }
  
  @Override
  public void close() {
    scanner.close();
  }
  
}
