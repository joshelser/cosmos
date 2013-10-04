package cosmos.trace;

import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.data.Mutation;

import cosmos.trace.Timings.TimedRegions.TimedRegion;

public class NullTracer extends Tracer {
  private static final String NULL_TRACE_ID = "null-trace-id";
  
  private static final NullTracer INSTANCE = new NullTracer();
  
  public static NullTracer instance() {
    return INSTANCE;
  }
  
  public NullTracer() {
    super(NULL_TRACE_ID);
  }
  
  @Override
  public void addTiming(TimedRegion timing) {
    return;
  }
  
  @Override
  public void addTiming(String description, Long duration) {
    return;
  }
  
  @Override
  public String getUUID() {
    return NULL_TRACE_ID; 
  }
  
  @Override
  public long getBegin() {
    return 0l;
  }
  
  @Override
  public List<TimedRegion> getTimings() {
    return Collections.emptyList();
  }
  
  @Override
  public List<Mutation> toMutations() {
    return Collections.emptyList();
  }
 
  @Override
  public boolean equals(Object o) {
    if (null == o) {
      return false;
    }
    
    return o instanceof NullTracer;
  }
  
  @Override
  public String toString() {
    return "NullTracer";
  }
}
