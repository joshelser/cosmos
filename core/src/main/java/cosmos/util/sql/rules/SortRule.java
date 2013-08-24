package cosmos.util.sql.rules;

import org.eigenbase.rel.ProjectRel;
import org.eigenbase.rel.SortRel;

import cosmos.util.sql.TableScanner;
import cosmos.util.sql.impl.CosmosTable;

/**
 * Initially the rule were separate; however, since 
 * they can be handled in a single class we simply use this class
 * to push the rules down the optimizer
 * @author phrocker
 *
 */
public class SortRule extends PushDownRule {

	CosmosTable accumuloAccessor;


	public SortRule(CosmosTable resultTable) {
		super(resultTable,some(ProjectRel.class, any(TableScanner.class)),"SorterShmorter");
		this.accumuloAccessor = resultTable;
	}
	
	
	

}