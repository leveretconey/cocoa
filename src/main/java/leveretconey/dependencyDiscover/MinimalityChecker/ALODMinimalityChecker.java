package leveretconey.dependencyDiscover.MinimalityChecker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Dependency.LexicographicalOrderDependency;
import leveretconey.dependencyDiscover.MinimalityChecker.LODMinimalityChecker;
import leveretconey.dependencyDiscover.Predicate.Operator;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicate;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicateList;
import leveretconey.util.Util;

public class ALODMinimalityChecker extends LODMinimalityChecker {

    private HashMap<Integer, Set<SingleAttributePredicateList>>
        right2Left=new HashMap<>();

    public int size=0;

    @Override
    public int size() {
        return size;
    }

    @Override
    public void insert(SingleAttributePredicateList left, SingleAttributePredicate right) {
        if (right.operator==Operator.greaterEqual){
            left=left.getReverseList();
        }
        Set<SingleAttributePredicateList> lists;
        if (right2Left.containsKey(right.attribute)){
            lists=right2Left.get(right.attribute);
        }else {
            lists=new HashSet<>();
            right2Left.put(right.attribute,lists);
        }
        size++;
        lists.add(left);
    }



    @Override
    public boolean isMinimal(SingleAttributePredicateList listToAdd,
                             SingleAttributePredicate predicateToAdd) {
        if (listToAdd.size()==0 || !right2Left.containsKey(predicateToAdd.attribute)){
            return true;
        }
        if (predicateToAdd.operator == Operator.greaterEqual) {
            listToAdd = listToAdd.getReverseList();
        }
        List<SingleAttributePredicate> trueListToAdd=listToAdd.list;
        HashMap<SingleAttributePredicate, Integer> predicate2positionInListToAdd
                = new HashMap<>();
        for (int i = 0; i < trueListToAdd.size(); i++) {
            predicate2positionInListToAdd.put(trueListToAdd.get(i), i);
        }

        nextLeft:
        for (SingleAttributePredicateList leftSAPL : right2Left.get(predicateToAdd.attribute)) {
            List<SingleAttributePredicate> left=leftSAPL.list;
            int begin = predicate2positionInListToAdd.getOrDefault(left.get(0),-1);
            if (begin==-1) {
                continue;
            }
            int remainLength=trueListToAdd.size()-begin;
            if (remainLength < left.size()){
                continue;
            }
            for(int i=0;i<left.size();i++){
                if (trueListToAdd.get(begin+i)!=left.get(i)){
                    continue nextLeft;
                }
            }
            return false;
        }
        return true;
    }


}
