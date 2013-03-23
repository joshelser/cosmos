package sorts.options;
import com.google.common.base.Preconditions;


public class Paging {
  protected final Integer pageSize;
  protected final Long maxResults;
  
  public Paging(Integer pageSize, Long maxResults) {
    Preconditions.checkNotNull(pageSize);
    Preconditions.checkNotNull(maxResults);
    
    Preconditions.checkArgument(0 < pageSize);
    Preconditions.checkArgument(0 < maxResults);
    
    this.pageSize = pageSize;
    this.maxResults = maxResults;
  }
  
  /**
   * @return the pageSize
   */
  public Integer pageSize() {
    return pageSize;
  }

  /**
   * @return the maxResults
   */
  public Long maxResults() {
    return maxResults;
  }
 
  public static Paging create(Integer pageSize, Long maxResults) {
    return new Paging(pageSize, maxResults);
  }
}
