package cosmos.util.sql.call;

import java.util.List;

import com.google.common.collect.Lists;

public class ChildVisitor implements CallIfc  {

	protected List<CallIfc> children;


	public ChildVisitor()
	{
		children = Lists.newArrayList();
	}
	
	@Override
	public CallIfc addChild(CallIfc child) {
		children.add(child);
		return this;
	}

	
	
	
}
