package leveretconey.dependencyDiscover.SPCache;

import leveretconey.dependencyDiscover.SortedPartition.SortedPartition;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicateList;

public interface SortedPartitionCache {
    public SortedPartition get(SingleAttributePredicateList list);
}
