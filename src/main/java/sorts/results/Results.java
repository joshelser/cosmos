package sorts.results;


/**
 * 
 */
public interface Results<T> extends Iterable<T> {
  // Copy the close off of ScannerBase
  public void close();
}
