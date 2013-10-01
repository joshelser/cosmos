package cosmos.statistics;

import static com.google.common.base.Preconditions.checkNotNull;

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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;

import cosmos.results.Column;
import cosmos.store.Store;

public class SchemaStatistics {

	private static final Logger log = LoggerFactory
			.getLogger(SchemaStatistics.class);
	private static Cache<Entry<Store, Column>, ColumnStatistics> statsCache = CacheBuilder
			.newBuilder().maximumSize(100).build();

	public static final Text STATE_COLFAM = new Text("state");

	public static ColumnStatistics getStatistics(Store store, Column column)
			throws TableNotFoundException {
		checkNotNull(store);

		ColumnStatistics stat = statsCache.getIfPresent(Maps.immutableEntry(
				store, column));

		if (null != stat) {
			Connector con = store.connector();

			Scanner s = con.createScanner(store.metadataTable(),
					Constants.NO_AUTHS);

			s.setRange(new Range(store.uuid()));

			s.fetchColumnFamily(STATE_COLFAM);

			Iterator<Entry<Key, Value>> iter = s.iterator();

			if (iter.hasNext()) {
				Entry<Key, Value> stateEntry = iter.next();
				return null;
				// return deserializeState(stateEntry.getValue());
			}

		}
		return stat;
	}

}
