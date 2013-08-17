package cosmos.util.sql.call.impl;

import cosmos.util.sql.call.CallIfc;

public class AccumuloCaller implements CallIfc{

	public CallIfc child;
	@Override
	public CallIfc addChild(CallIfc child) {
		this.child = child;
		return this;
	}
	
	

}
