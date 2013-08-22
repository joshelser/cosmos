package cosmos.util.sql.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;

import com.google.common.base.Function;
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
import cosmos.util.sql.FlatQueryPlanner;
import cosmos.util.sql.ResultDefiner;
import cosmos.util.sql.call.FilterIfc;
import cosmos.util.sql.call.impl.FieldEquality;
import cosmos.util.sql.call.impl.Filter;
import cosmos.util.sql.impl.functions.FieldLimiter;

public class CosmosSql extends ResultDefiner {

	/**
	 * Sortable result set reference.
	 */
	protected SortableResult sort;

	/**
	 * Main cosmos reference
	 */
	protected Cosmos cosmos;

	protected CloseableIterable<MultimapQueryResult> iter;

	/**
	 * Iterator reference
	 */
	protected Iterator<MultimapQueryResult> baseIter;

	/**
	 * Constructor
	 */

	public CosmosSql(SortableResult results, Cosmos cosmosImpl)
			throws MutationsRejectedException, TableNotFoundException,
			UnexpectedStateException {
		this.sort = results;
		this.cosmos = cosmosImpl;
		cosmos.register(sort);
	}

	@Override
	public String getDataTable() {
		return sort.dataTable();
	}

	@Override
	public Set<Index> getIndexColumns() {
		return sort.columnsToIndex();
	}

	@Override
	public AccumuloIterables<Object[]> iterator(List<String> schemaLayout,
			AccumuloRel.Planner planner) {
		Iterator<Object[]> returnIter = Iterators.emptyIterator();

		
		if (planner instanceof FlatQueryPlanner) {

			FlatQueryPlanner query = (FlatQueryPlanner) planner;
			Filter filterImpl = query.getFilter();

			if (null != filterImpl) {
				System.out.println("oh" );
				List<FilterIfc> filters = filterImpl.getFilters();

				cosmos.util.sql.call.Fields limitField = query.getLimitFields();

				for (FilterIfc filter : filters) {
					if (filter instanceof FieldEquality) {
						FieldEquality equality = (FieldEquality) filter;

						cosmos.util.sql.call.Field field = equality.getField();
						cosmos.util.sql.call.Literal literal = equality
								.getLiteral();
						System.out.println(field.toString() + "="
								+ literal.toString() + "_");

						try {
							iter = cosmos.fetch(sort,
									new Column(field.toString()),
									literal.toString());

							baseIter = iter.iterator();
						} catch (TableNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (UnexpectedStateException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (UnindexedColumnException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

					}
				}
				baseIter = Iterators.transform(baseIter,
						new FieldLimiter(limitField.getFields()));
System.out.println("oh baby"  + baseIter.hasNext());
				returnIter = Iterators.transform(baseIter,
						new DocumentExpansion(schemaLayout));
			}

		}
		return new AccumuloIterables<Object[]>(returnIter);
	}

	private class DocumentExpansion implements
			Function<MultimapQueryResult, Object[]> {

		private List<String> fields;

		public DocumentExpansion(List<String> fields) {
			this.fields = fields;
		}

		public Object[] apply(MultimapQueryResult document) {

			System.out.println("convert");
			Object[] results = new List[fields.size()];

			for (int i = 0; i < fields.size(); i++) {
				String field = fields.get(i);
				Collection<SValue> values = document.get(new Column(field));
				System.out.println("convert " + field + " " + values.size() );
				if (values != null) {
					//new cosmos.util.sql.Column("");
					List<SValue> columns = Lists.newArrayList();
					columns.addAll(values);
					results[i] = columns;//values.iterator().next().value();
					
				} else {
					results[i] = new ArrayList< SValue>();//values.iterator().next().value();
					//results[i] = new cosmos.util.sql.Column("");//new String[] {};
				}
			}
			return results;

		}

	}

}
