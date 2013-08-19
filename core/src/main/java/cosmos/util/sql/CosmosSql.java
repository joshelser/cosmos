package cosmos.util.sql;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import cosmos.Cosmos;
import cosmos.UnexpectedStateException;
import cosmos.UnindexedColumnException;
import cosmos.impl.SortableResult;
import cosmos.options.Index;
import cosmos.results.CloseableIterable;
import cosmos.results.Column;
import cosmos.results.impl.MultimapQueryResult;
import cosmos.util.sql.call.FilterIfc;
import cosmos.util.sql.call.impl.FieldEquality;
import cosmos.util.sql.call.impl.Filter;
import cosmos.util.sql.impl.functions.FieldLimiter;

public class CosmosSql implements SchemaDefiner<MultimapQueryResult>{
	
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
	public CosmosSql(SortableResult results, Cosmos cosmosImpl) throws MutationsRejectedException, TableNotFoundException, UnexpectedStateException
	{
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
	public AccumuloIterables<MultimapQueryResult> iterator(SelectQuery query) {
		
		Filter filterImpl = query.getFilter();
		
		List<FilterIfc> filters = filterImpl.getFilters();
		
		cosmos.util.sql.call.Field limitField = query.getLimitFields();
		
		
		
		for(FilterIfc filter : filters)
		{
			if (filter instanceof FieldEquality)
			{
				FieldEquality equality = (FieldEquality)filter;
				
				cosmos.util.sql.call.Field field = equality.getField();
				cosmos.util.sql.call.Literal literal = equality.getLiteral();
				System.out.println(field.toString() + "=" + literal.toString()+"_");
				
				try {
					iter =  cosmos.fetch(sort, new Column(field.toString()), literal.toString());
					
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
		baseIter = Iterators.transform(baseIter,new FieldLimiter(Lists.newArrayList(limitField)));
		
		return new AccumuloIterables<MultimapQueryResult>(baseIter  );
	}
	
	
}
