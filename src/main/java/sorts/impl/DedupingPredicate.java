package sorts.impl;

import java.nio.charset.CharacterCodingException;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import sorts.options.Defaults;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Sets;

/**
 * This is apt to be stupidly slow. Perhaps use a bloomfilter instead?
 * Perhaps the API itself should just allow the client to specify when it
 * cares about getting dupes.
 */
public class DedupingPredicate implements Predicate<Entry<Key,Value>> {

  protected final Set<String> uids;
  private final Text holder;
  
  public DedupingPredicate() {
    uids = Sets.newHashSetWithExpectedSize(64);
    holder = new Text();
  }
  
  @Override
  public boolean apply(Entry<Key,Value> input) {
    Preconditions.checkNotNull(input);
    Preconditions.checkNotNull(input.getKey());
    
    input.getKey().getColumnQualifier(holder);
    
    int index = holder.find(Defaults.NULL_BYTE_STR);
    
    Preconditions.checkArgument(-1 != index);
    
    String uid = null;
    try {
      uid = Text.decode(holder.getBytes(), index + 1, holder.getLength() - (index + 1));
    } catch (CharacterCodingException e) {
      throw new RuntimeException(e);
    }
    
    // If we haven't seen this UID yet, note such, and then keep this item
    if (!uids.contains(uid)) {
      uids.add(uid);
      return true;
    }
    
    // Otherwise, don't re-return this item
    return false;
  }
  
}
