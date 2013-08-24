package cosmos.util.sql.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.log4j.Logger;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeFactory.FieldInfoBuilder;

import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import cosmos.Cosmos;
import cosmos.UnexpectedStateException;
import cosmos.UnindexedColumnException;
import cosmos.impl.SortableResult;
import cosmos.options.Index;
import cosmos.results.CloseableIterable;
import cosmos.results.Column;
import cosmos.results.SValue;
import cosmos.results.impl.MultimapQueryResult;
import cosmos.util.sql.AccumuloIterables;
import cosmos.util.sql.AccumuloRel;
import cosmos.util.sql.AccumuloSchema;
import cosmos.util.sql.AccumuloTable;
import cosmos.util.sql.ResultDefiner;
import cosmos.util.sql.TableDefiner;
import cosmos.util.sql.call.CallIfc;
import cosmos.util.sql.call.ChildVisitor;
import cosmos.util.sql.call.Field;
import cosmos.util.sql.call.Fields;
import cosmos.util.sql.call.FilterIfc;
import cosmos.util.sql.call.Literal;
import cosmos.util.sql.call.Pair;
import cosmos.util.sql.call.impl.FieldEquality;
import cosmos.util.sql.call.impl.Filter;
import cosmos.util.sql.impl.functions.FieldLimiter;

public class CosmosSql extends ResultDefiner implements TableDefiner {

	/**
	 * Main cosmos reference
	 */
	protected Cosmos cosmos;

	protected Iterable<MultimapQueryResult> iter;

	protected Collection<CloseableIterable<MultimapQueryResult>> plannedParentHood;

	/**
	 * Iterator reference
	 */
	protected Iterator<MultimapQueryResult> baseIter;

	private JavaTypeFactory typeFactory;

	private AccumuloSchema<?> schema;

	private static final Logger log = Logger.getLogger(CosmosSql.class);

	protected Cache<String, CosmosTable> tableCache = CacheBuilder.newBuilder()
			.expireAfterAccess(24, TimeUnit.HOURS).build();

	/**
	 * Constructor
	 */

	public CosmosSql(Cosmos cosmosImpl) throws MutationsRejectedException,
			TableNotFoundException, UnexpectedStateException {
		plannedParentHood = Lists.newArrayList();
		iter = Collections.emptyList();
		this.cosmos = cosmosImpl;

	}

	@Override
	public String getDataTable() {
		return "";

	}

	@Override
	public AccumuloIterables<Object[]> iterator(List<String> schemaLayout,
			AccumuloRel.Plan planner) {
		Iterator<Object[]> returnIter = Iterators.emptyIterator();

		ChildVisitor<? extends CallIfc<?>> query = planner.getChildren();

		Collection<Filter> filters = (Collection<Filter>) query
				.children(Filter.class.getSimpleName());

		Collection<Fields> fields = (Collection<Fields>) query
				.children("selectedFields");

		String table = ((CosmosTable) planner.table).getTable();

		SortableResult res;
		try {
			res = cosmos.fetch(table);

			if (res != null) {
				for (Filter filter : filters) {

					for (FilterIfc subFilter : filter.getFilters()) {
						if (subFilter instanceof FieldEquality) {
							FieldEquality equality = (FieldEquality) subFilter;

							for (Pair<Field, Literal> entry : equality
									.getChildren()) {

								cosmos.util.sql.call.Field field = (Field) entry
										.first();
								cosmos.util.sql.call.Literal literal = (Literal) entry
										.second();

								try {
									plannedParentHood.add(cosmos.fetch(res,
											new Column(field.toString()),
											literal.toString()));

								} catch (TableNotFoundException e) {
									log.error(e);
								} catch (UnexpectedStateException e) {
									log.error(e);
								} catch (UnindexedColumnException e) {
									log.error(e);
								}
							}
						}
					}
				}

				for (CloseableIterable<MultimapQueryResult> subIter : plannedParentHood) {
					if (iter == null) {
						iter = subIter;
					} else {
						iter = Iterables.concat(iter, subIter);
					}
				}
			}
		} catch (UnexpectedStateException e1) {
			log.error(e1);
		}

		List<Field> fieldsUserWants = Lists.newArrayList();

		for (Fields fieldList : fields) {
			fieldsUserWants.addAll(fieldList.getFields());
		}

		baseIter = iter.iterator();
		baseIter = Iterators.transform(baseIter, new FieldLimiter(
				fieldsUserWants));

		returnIter = Iterators.transform(baseIter, new DocumentExpansion(
				schemaLayout));

		return new AccumuloIterables<Object[]>(returnIter);
	}

	class DocumentExpansion implements Function<MultimapQueryResult, Object[]> {

		private List<String> fields;

		public DocumentExpansion(List<String> fields) {
			this.fields = fields;
		}

		public Object[] apply(MultimapQueryResult document) {

			Object[] results = new List[fields.size()];

			for (int i = 0; i < fields.size(); i++) {
				String field = fields.get(i);
				Collection<SValue> values = document.get(new Column(field));
				if (values != null) {

					List<SValue> columns = Lists.newArrayList();
					columns.addAll(values);
					results[i] = columns;// values.iterator().next().value();

				} else {
					results[i] = new ArrayList<SValue>();
				}
			}
			return results;

		}

	}

	@Override
	public AccumuloTable<?> getTable(String name) {
		CosmosTable table = tableCache.getIfPresent(name);
		try {

			if (table == null) {
				SortableResult sort = cosmos.fetch(name);

				FieldInfoBuilder builder = new RelDataTypeFactory.FieldInfoBuilder();

				for (Index indexField : sort.columnsToIndex()) {
					builder.add(indexField.column().column(),
							typeFactory.createType(indexField.getIndexTyped()));
				}

				final RelDataType rowType = typeFactory
						.createStructType(builder);

				try {
					table = new CosmosTable(schema, this, typeFactory, rowType,
							name);

					tableCache.put(name, table);
				} catch (SecurityException e) {
					log.error(e);
				}
			}

		} catch (UnexpectedStateException e) {
			log.error(e);

		}

		return table;
	}

	@Override
	public Set<Index> getIndexColumns(String table) {
		SortableResult sort;
		try {
			sort = cosmos.fetch(table);

			if (sort != null) {
				return sort.columnsToIndex();
			}

		} catch (UnexpectedStateException e) {
			log.error(e);
		}
		return Collections.emptySet();
	}

	@Override
	public void register(AccumuloSchema<?> parentSchema) {
		this.schema = parentSchema;
		this.typeFactory = parentSchema.getTypeFactory();

	}

}
