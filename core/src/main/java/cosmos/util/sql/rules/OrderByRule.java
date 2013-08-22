package cosmos.util.sql.rules;

import org.eigenbase.rel.AggregateRel;

import cosmos.util.sql.TableScanner;
import cosmos.util.sql.impl.CosmosTable;

/**
 * Initially the rule were separate; however, since 
 * they can be handled in a single class we simply use this class
 * to push the rules down the optimizer
 * @author phrocker
 *
 */
public class OrderByRule extends PushDownRule {

	CosmosTable accumuloAccessor;


	public OrderByRule(CosmosTable resultTable) {
		super(resultTable,any(AggregateRel.class));
		this.accumuloAccessor = resultTable;
	}
	
	
	

}