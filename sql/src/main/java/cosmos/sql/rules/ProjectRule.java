package cosmos.sql.rules;

import org.eigenbase.rel.SortRel;

import cosmos.sql.TableScanner;
import cosmos.sql.impl.CosmosTable;

/**
 * Initially the rule were separate; however, since 
 * they can be handled in a single class we simply use this class
 * to push the rules down the optimizer
 * @author phrocker
 *
 */
public class ProjectRule extends PushDownRule {

	CosmosTable accumuloAccessor;


	public ProjectRule(CosmosTable resultTable) {
		super(resultTable,some(SortRel.class, any(TableScanner.class)),"ProjectShmore");
		this.accumuloAccessor = resultTable;
	}
	
	
	

}