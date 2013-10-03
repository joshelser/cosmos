package cosmos.statistics;

import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import cosmos.store.Store;

public class StatsCache {
  private static long cacheTimeout = 10;

  private static TimeUnit durationUnits;

  private static int maxSize = 100;

  public static Cache<Entry<Store,String>,StatisticsIfc> cache;

  static {
    initialize();
  }

  public static void setCacheTimeout(long time, TimeUnit units) {
    cacheTimeout = time;
    durationUnits = units;
    initialize();
  }

  protected static void initialize() {
    cache = CacheBuilder.newBuilder().expireAfterAccess(cacheTimeout, durationUnits).maximumSize(maxSize).build();
  }
}
