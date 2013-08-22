package cosmos.util.sql.call;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

public class Fields implements CallIfc {

	protected List<Field> fields;

	public Fields(List<String> literal) {
		fields = Lists.newArrayListWithCapacity(literal.size());

		for (String field : literal) {
			addChild(new Field(field));
		}
	}

	@Override
	public CallIfc addChild(CallIfc child) {
		if (child instanceof Field) {
			fields.add((Field) child);
		}
		return this;
	}

	public List<Field> getFields() {
		return Collections.unmodifiableList(fields);
	}

}
