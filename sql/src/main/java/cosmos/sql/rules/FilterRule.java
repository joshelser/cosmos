package cosmos.sql.rules;

import org.eigenbase.rel.FilterRel;

import cosmos.sql.TableScanner;
import cosmos.sql.impl.CosmosTable;

/**
 * Initially the rule were separate; however, since 
 * they can be handled in a single class we simply use this class
 * to push the rules down the optimizer
 * @author phrocker
 *
 */
public class FilterRule extends PushDownRule {

	CosmosTable accumuloAccessor;


	public FilterRule(CosmosTable resultTable) {
		super(resultTable,some(FilterRel.class,any(TableScanner.class)),"FilterShmilter");
		this.accumuloAccessor = resultTable;
	}
	
	
	

}