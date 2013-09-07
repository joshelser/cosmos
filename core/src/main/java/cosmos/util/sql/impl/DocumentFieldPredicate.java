package cosmos.util.sql.impl;

import java.util.Collection;

import com.google.common.base.Predicate;

import cosmos.results.Column;
import cosmos.results.SValue;
import cosmos.results.impl.MultimapQueryResult;
import cosmos.util.sql.call.Field;
import cosmos.util.sql.call.Literal;

public class DocumentFieldPredicate implements Predicate<MultimapQueryResult> {

	private Column column;
	private Literal predicateValue;

	public DocumentFieldPredicate(Field field, Literal value) {
		this.column = new Column(field.toString());
		this.predicateValue = value;
	}

	@Override
	public boolean apply(MultimapQueryResult input) {
		Collection<SValue> values = input.get(column);
		for (SValue value : values) {
			if (value.value().equals(predicateValue.toString()))
				return true;
		}

		return false;
	}

}
