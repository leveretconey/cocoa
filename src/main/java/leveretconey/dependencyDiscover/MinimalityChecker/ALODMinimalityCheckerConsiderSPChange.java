package leveretconey.dependencyDiscover.MinimalityChecker;

import leveretconey.cocoa.twoSideExpand.TwoSideDFSSPCache;
import leveretconey.dependencyDiscover.SortedPartition.SortedPartition;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicate;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicateList;

public class ALODMinimalityCheckerConsiderSPChange extends LODMinimalityChecker {

    private ALODMinimalityChecker embeddedMinimalityChecker
            =new ALODMinimalityChecker();
    private TwoSideDFSSPCache spCache;

    @Override
    public int size() {
        return embeddedMinimalityChecker.size();
    }

    public ALODMinimalityCheckerConsiderSPChange(TwoSideDFSSPCache spCache) {
        this.spCache = spCache;
    }

    @Override
    public void insert(SingleAttributePredicateList left, SingleAttributePredicate right) {
        embeddedMinimalityChecker.insert(left, right);
    }

    @Override
    public boolean isMinimal(SingleAttributePredicateList listToAdd, SingleAttributePredicate predicateToAdd) {
        if (!embeddedMinimalityChecker.isMinimal(listToAdd, predicateToAdd)){
            return false;
        }
        if (listToAdd.size()==0){
            return true;
        }
        SortedPartition sp=spCache.get(listToAdd);
        SingleAttributePredicateList listPlus = listToAdd.deepCloneAndAdd(predicateToAdd);
        SortedPartition spPlus=spCache.get(listPlus);
        if (sp.equalsFast(spPlus)){
            embeddedMinimalityChecker.insert(listToAdd, predicateToAdd);
            spCache.mayRemove(listPlus);
            return false;
        }
        return true;

    }


}
