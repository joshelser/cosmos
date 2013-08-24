package cosmos.util.sql.call;

import java.util.Collection;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class ChildVisitor<T extends CallIfc> implements CallIfc<T> {

	protected Multimap<String, T> children;

	public ChildVisitor() {
		children = ArrayListMultimap.create();
	}

	@Override
	public CallIfc<?> addChild(String id, T child) {
		children.put(id, child);
		return this;
	}

	public Collection<T> children(String id) {
		return children.get(id);
	}

	public Collection<String> childrenIds() {
		return children.keySet();
	}

}
