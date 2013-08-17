package cosmos.util.sql.impl.functions;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.security.ColumnVisibility;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import cosmos.results.Column;
import cosmos.results.SValue;
import cosmos.results.impl.MultimapQueryResult;
import cosmos.util.sql.call.Field;

public class FieldLimiter implements
		Function<MultimapQueryResult, MultimapQueryResult> {

	Predicate<Entry<Column, SValue>> limitingPredicate;

	public FieldLimiter(List<Field> fields) {
		limitingPredicate = new FieldLimitPredicate(
				Lists.newArrayList(Iterables.transform(fields,
						new Function<Field, String>() {
							@Override
							public String apply(Field field) {
								return field.toString();
							}
						})));

	}

	@Override
	public MultimapQueryResult apply(MultimapQueryResult input) {

		// TODO Auto-generated method stub
		return new FieldLimitingQueryResult(input, input.docId(),limitingPredicate);
	}

	private class FieldLimitingQueryResult extends MultimapQueryResult {
		public FieldLimitingQueryResult(MultimapQueryResult other,
				String newDocId, Predicate<Entry<Column, SValue>> limitingPredicate) {
			super(other, newDocId);
			document = Multimaps.filterEntries(document,limitingPredicate );
			
		}
	}

	private class FieldLimitPredicate implements
			Predicate<Entry<Column, SValue>> {

		private List<String> fields;

		public FieldLimitPredicate(List<String> fields) {
			this.fields = fields;
		}

		@Override
		public boolean apply(Entry<Column, SValue> entry) {
			return fields.contains(entry.getKey().column());
		}
	}

}
