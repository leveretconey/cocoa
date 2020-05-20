package leveretconey.dependencyDiscover.MinimalityChecker;

import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicate;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicateList;

public class AlwaysOpenMinimalityChecker extends LODMinimalityChecker {
    @Override
    public int size() {
        return 0;
    }

    @Override
    public void insert(SingleAttributePredicateList left, SingleAttributePredicate right) {

    }

    @Override
    public boolean isMinimal(SingleAttributePredicateList listToAdd, SingleAttributePredicate predicateToAdd) {
        return true;
    }
}
