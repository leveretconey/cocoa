package leveretconey.dependencyDiscover.MinimalityChecker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import leveretconey.dependencyDiscover.Predicate.Operator;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicate;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicateList;
import leveretconey.fastod.AttributeSet;

public class ALODMinimalityCheckerUseFD extends LODMinimalityChecker {
    private HashMap<Integer, Set<Long>>
        right2Left=new HashMap<>();

    public int size=0;

    @Override
    public int size() {
        return size;
    }

    @Override
    public void insert(SingleAttributePredicateList left, SingleAttributePredicate right) {
        Set<Long> lists;
        if (right2Left.containsKey(right.attribute)){
            lists=right2Left.get(right.attribute);
        }else {
            lists=new HashSet<>();
            right2Left.put(right.attribute,lists);
        }
        size++;
        lists.add(listToBitSet(left));
    }



    @Override
    public boolean isMinimal(SingleAttributePredicateList listToAdd,
                             SingleAttributePredicate predicateToAdd) {
        if (listToAdd.size()==0 || !right2Left.containsKey(predicateToAdd.attribute)) {
            return true;
        }

        long listToAddBitSet=listToBitSet(listToAdd);
        for (long fdLeft : right2Left.get(predicateToAdd.attribute)) {
            if ( (fdLeft| listToAddBitSet)==listToAddBitSet){
                return false;
            }
        }
        return true;
    }

    protected long listToBitSet(SingleAttributePredicateList list){
        long result=0;
        for (SingleAttributePredicate predicate : list) {
            result=result | (1<<predicate.attribute) ;
        }
        return result;
    }

}
