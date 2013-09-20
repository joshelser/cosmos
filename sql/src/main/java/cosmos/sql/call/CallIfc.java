package cosmos.sql.call;

@SuppressWarnings("rawtypes")
public interface CallIfc<T extends CallIfc> {
  
  public CallIfc addChild(String id, T child);
}
