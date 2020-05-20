package leveretconey.cocoa.sample;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import leveretconey.cocoa.twoSideExpand.ALODTree.ALODTreeNode;
import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.MinimalityChecker.LODMinimalityChecker;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicate;

public class ExpandPredicateProvider {

    private DataFrame data;
    private LODMinimalityChecker minimalityChecker;
    private Map<ALODTreeNode, List<SingleAttributePredicate>>
        predicateCache=new HashMap<>();

    public ExpandPredicateProvider(DataFrame data, LODMinimalityChecker minimalityChecker) {
        this.data = data;
        this.minimalityChecker = minimalityChecker;
    }

    public void resetPredicates(ALODTreeNode node){
        List<SingleAttributePredicate> expandPredicates;
        if (!node.willExpand()){
            expandPredicates=new ArrayList<>();
        }
        else {
            expandPredicates = node.toLOD().getExpandPredicates(data, minimalityChecker, node.isExpandLeft());
        }
        predicateCache.put(node,expandPredicates);
    }

    public void addPredicate(ALODTreeNode node, SingleAttributePredicate predicate){
        if (!predicateCache.containsKey(node)){
            predicateCache.put(node,new ArrayList<>());
        }
        predicateCache.get(node).add(predicate);
    }

    public SingleAttributePredicate getNextPredicate(ALODTreeNode node){
        if (!predicateCache.containsKey(node)){
            resetPredicates(node);
        }

        List<SingleAttributePredicate> predicates = predicateCache.get(node);
        if (predicates.size()==0){
            return null;
        }else {
            return predicates.get(predicates.size()-1);
        }
    }

    public void pollNextPredicate(ALODTreeNode node){
        List<SingleAttributePredicate> predicates = predicateCache.get(node);
        predicates.remove(predicates.size()-1);
    }

    public void removePredicateCache(ALODTreeNode node){
        predicateCache.remove(node);
    }
}
