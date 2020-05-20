package leveretconey.dependencyDiscover.MinimalityChecker;

import leveretconey.dependencyDiscover.Dependency.LexicographicalOrderDependency;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicate;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicateList;

public abstract class LODMinimalityChecker implements MinimalityChecker<LexicographicalOrderDependency> {

    public abstract int size();

    public final void insert(LexicographicalOrderDependency dependency){
        insert(dependency.left,dependency.right.get(0));
    }
    public abstract void insert(SingleAttributePredicateList left,SingleAttributePredicate right);

    @Override
    public final boolean isMinimal(LexicographicalOrderDependency dependency) {
        return isMinimal(dependency.left) && isMinimal(dependency.right);
    }

    public final boolean isMinimal(SingleAttributePredicateList list){
        SingleAttributePredicateList oneLess = list.deepCloneAndRemoveLast();
        return isMinimal(oneLess,list.list.get(list.size()-1));
    }

    public abstract boolean isMinimal(SingleAttributePredicateList listToAdd,
                             SingleAttributePredicate predicateToAdd);
}
