package cosmos.statistics;

import static com.google.common.base.Preconditions.checkNotNull;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import cosmos.options.Index;
import cosmos.statistics.store.Cardinality;
import cosmos.statistics.store.Count;
import cosmos.statistics.store.Statistic;
import cosmos.store.Store;

public class SchemaStatistics {

  private static final Logger log = LoggerFactory.getLogger(SchemaStatistics.class);

  public static final Text STATS_COLFAM = new Text("stats");

  public static final Text COUNT_COLUMN = new Text(STATS_COLFAM + "/count");

  // stats/index/<type>/<column>
  public static final Text INDEX_STATS_COLUMN = new Text(STATS_COLFAM + "/index");

  public static final String CARDINALITY_COLUMN = "cardinality";

  public static IndexStatistics getStatistics(Store store, Index column) throws TableNotFoundException {
    checkNotNull(store);

    IndexStatistics stat = (IndexStatistics) StatsCache.cache.getIfPresent(Maps.immutableEntry(store, column));

    if (null != stat) {

      stat = new IndexStatistics(store, column);

      stat.setCardinality(getCardinality(store, column));

      stat.setCountEstimate(getCountEstimate(store, column));

      // cache the current value
      StatsCache.cache.put(Maps.immutableEntry(store, column.column().toString()), (StatisticsIfc) stat);
    }
    return stat;
  }

  public static Cardinality getCardinality(Store store, Index column) throws TableNotFoundException {

    
    Cardinality runningCardinality = new Cardinality(0);
    Text columnFamily = new Text(INDEX_STATS_COLUMN + "/" + CARDINALITY_COLUMN + "/" + column.column().toString());
    aggregateStatistic(store,columnFamily,runningCardinality);
    return runningCardinality;
  }

  protected static void aggregateStatistic(Store store, Text columnFamily, Statistic stat) throws TableNotFoundException {

    checkNotNull(store);
    checkNotNull(columnFamily);
    checkNotNull(stat);

    Connector con = store.connector();

    Scanner s = con.createScanner(store.metadataTable(), Constants.NO_AUTHS);

    s.setRange(new Range(store.uuid()));

    s.fetchColumnFamily(COUNT_COLUMN);

    Iterator<Entry<Key,Value>> iter = s.iterator();

    Class<? extends Statistic> statClazz = stat.getClass();

    if (iter.hasNext()) {

      Text cq = iter.next().getKey().getColumnQualifier();
      Statistic otherStat;
      try {
        otherStat = statClazz.getConstructor(String.class).newInstance(cq.toString());
        stat.aggregate(otherStat);
      } catch (IllegalArgumentException e) {
        log.error(e.toString());
      } catch (SecurityException e) {
        log.error(e.toString());
      } catch (InstantiationException e) {
        log.error(e.toString());
      } catch (IllegalAccessException e) {
        log.error(e.toString());
      } catch (InvocationTargetException e) {
        log.error(e.toString());
      } catch (NoSuchMethodException e) {
        log.error(e.toString());
      }

    }

  }

  public static Count getCountEstimate(Store store, Index column) throws TableNotFoundException {
    Count count = new Count(0);
    aggregateStatistic(store,COUNT_COLUMN,count);
    return count;
  }

}
