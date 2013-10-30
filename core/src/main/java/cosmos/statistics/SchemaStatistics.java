package cosmos.statistics;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import cosmos.options.Index;
import cosmos.results.Column;
import cosmos.statistics.store.Cardinality;
import cosmos.statistics.store.Count;
import cosmos.statistics.store.Statistic;
import cosmos.store.Store;

public class SchemaStatistics {

	private static final Logger log = LoggerFactory
			.getLogger(SchemaStatistics.class);

	public static final Text STATS_COLFAM = new Text("stats");

	public static final Text COUNT_COLUMN = new Text(STATS_COLFAM + "/count");

	// stats/index/<type>/<column>
	public static final Text INDEX_STATS_COLUMN = new Text(STATS_COLFAM
			+ "/index");

	public static final Text CARDINALITY_COLUMN = new Text(STATS_COLFAM + "/cardinality");

	public static IndexStatistics getStatistics(Store store, Index column)
			throws TableNotFoundException {
		checkNotNull(store);

		IndexStatistics stat = (IndexStatistics) StatsCache.cache
				.getIfPresent(Maps.immutableEntry(store, column));

		if (null == stat) {

			stat = new IndexStatistics(store, column);

			stat.setCardinality(getCardinality(store, column));

			stat.setCountEstimate(getCountEstimate(store, column));

			// cache the current value
			StatsCache.cache.put(
					Maps.immutableEntry(store, column.column().toString()),
					(StatisticsIfc) stat);
		}
		return stat;
	}

	public static IndexStatistics storeAndUpdate(Store store, Index column)
			throws TableNotFoundException {
		// stores and caches stats for a given document
		IndexStatistics currentStats = getStatistics(store, column);

		return currentStats;
	}

	public static Cardinality getCardinality(Store store, Index column)
			throws TableNotFoundException {

		Cardinality runningCardinality = new Cardinality(Integer.valueOf(4));
		Text columnFamily = new Text(INDEX_STATS_COLUMN + "/"
				+ CARDINALITY_COLUMN + "/" + column.column().toString());
		aggregateStatistic(store, columnFamily, runningCardinality, column.column());
		return runningCardinality;
	}

	protected static void aggregateStatistic(Store store, Text columnFamily,
			Statistic stat, Column column) throws TableNotFoundException {

		checkNotNull(store);
		checkNotNull(columnFamily);
		checkNotNull(stat);

		Connector con = store.connector();

		Scanner s = con
				.createScanner(store.metadataTable(), Constants.NO_AUTHS);

		Key startKey = new Key(new Text(store.uuid()),columnFamily,new Text(column.name()));
		
		s.setRange(new Range(startKey,true,startKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL),false));

		s.fetchColumnFamily(columnFamily);

		Iterator<Entry<Key, Value>> iter = s.iterator();

		Class<? extends Statistic> statClazz = stat.getClass();

		if (iter.hasNext()) {

			Entry<Key,Value> kv = iter.next();
			Text cq = kv.getKey().getColumnQualifier();
			String value = new String( kv.getValue().get() );
			Statistic otherStat;
			try {
				otherStat = statClazz.getConstructor(String.class).newInstance(
						value);
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

	public static Count getCountEstimate(Store store, Index column)
			throws TableNotFoundException {
		Count count = new Count(0);
		aggregateStatistic(store, COUNT_COLUMN, count, column.column());
		return count;
	}
	
	/**
	 * Update cached index statistics with the incoming statistics
	 * @param store
	 * @param column
	 * @param stats
	 * @return
	 * @throws IOException 
	 */
	public static IndexStatistics storeAndUpdate(Store store, Index column, IndexStatistics stat) throws TableNotFoundException, IOException
	{
		checkNotNull(store);
		checkNotNull(column);
		checkNotNull(stat);
		
		// aggregate the statistics, then store them
		IndexStatistics myStat = getStatistics(store, column);
		
		myStat.countEstimate().aggregate( stat.countEstimate() );
		
		myStat.cardinality().aggregate( stat.cardinality() );

		Connector con = store.connector();
		BatchWriterConfig config = new BatchWriterConfig();
		
		config.setMaxLatency(10L, TimeUnit.MILLISECONDS);
		config.setMaxMemory(1024L*1024L*2L);

		BatchWriter writer = con.createBatchWriter(store.metadataTable(), config);
		
		Mutation m = new Mutation(store.uuid());
		
		m.put(COUNT_COLUMN, new Text( column.column().name() ), new Value( myStat.countEstimate().toString().getBytes() ) );
		
		m.put(CARDINALITY_COLUMN, new Text( column.column().name() ), new Value( myStat.cardinality().toString().getBytes() ) );
		
		StatsCache.cache.put(Maps.immutableEntry(store, column.column().name()), myStat);
		
		try {
			writer.addMutation(m);
			
			writer.close();
		} catch (MutationsRejectedException e) {
			throw new IOException(e);
		}
		return myStat;
		
		
	}

	/**
	 * @param meataData
	 * @param columns
	 * @throws TableNotFoundException 
	 */
	public static Collection<IndexStatistics> storeAndUpdate(Store store,
			Collection<Index> columns) throws TableNotFoundException {
		Collection<IndexStatistics> rowStats = Lists.newArrayList();
		for (Index column : columns) {
			rowStats.add(storeAndUpdate(store, column));
		}

		return rowStats;
	}

}
