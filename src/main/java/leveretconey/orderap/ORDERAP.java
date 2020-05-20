package leveretconey.orderap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javax.sql.rowset.Predicate;

import leveretconey.cocoa.multipleStandard.DFSDiscovererWithMultipleStandard;
import leveretconey.cocoa.twoSideExpand.ALODTree;
import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Dependency.LexicographicalOrderDependency;
import leveretconey.dependencyDiscover.Discoverer.ALODDiscoverer;
import leveretconey.dependencyDiscover.MinimalityChecker.ALODMinimalityChecker;
import leveretconey.dependencyDiscover.MinimalityChecker.ALODMinimalityCheckerUseFD;
import leveretconey.dependencyDiscover.MinimalityChecker.LODMinimalityChecker;
import leveretconey.dependencyDiscover.Predicate.Operator;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicate;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicateList;
import leveretconey.dependencyDiscover.SPCache.SortedPartitionCache;
import leveretconey.dependencyDiscover.SortedPartition.ImprovedTwoSideSortedPartition;
import static leveretconey.cocoa.multipleStandard.DFSDiscovererWithMultipleStandard.*;

public class ORDERAP extends ALODDiscoverer {

    private ValidatorType type= ValidatorType.G1;
    private DataFrame data;
    LODMinimalityChecker minimalityChecker;

    @Override
    public Collection<LexicographicalOrderDependency> discover(DataFrame data, double errorRateThreshold) {
        this.data = data;
        List<LexicographicalOrderDependency> result=new ArrayList<>();
        SortedPartitionCache spCache=new ORDERSortedPartitionCache(data);
        LinkedList<LexicographicalOrderDependency> queue=new LinkedList<>();
        minimalityChecker=new ALODMinimalityCheckerUseFD();

        while (!queue.isEmpty()){
            LexicographicalOrderDependency parent = queue.pollFirst();
            ImprovedTwoSideSortedPartition isp=new ImprovedTwoSideSortedPartition
                    (spCache.get(parent.left),spCache.get(parent.right));
            double errorRate;
            if (type==ValidatorType.G1){
                errorRate=isp.validateForALODWithG1().errorRate;
            }else {
                errorRate=isp.validateForALODWithG3().errorRate;
            }
            if (errorRate>errorRateThreshold){
                for (SingleAttributePredicate expandPredicate : getExpandPredicates(parent.left,parent.right)) {
                    SingleAttributePredicateList expandList=parent.left.deepCloneAndAdd(expandPredicate);
                    if (minimalityChecker.isMinimal(expandList)){
                        continue;
                    }
                    if (spCache.get(expandList).equalsFast(spCache.get(parent.left))){
                        continue;
                    }
                    queue.addLast(new LexicographicalOrderDependency(expandList,parent.right));
                }
            }else {
                result.add(parent);
                for (SingleAttributePredicate expandPredicate : getExpandPredicates(parent.right,parent.left)) {
                    SingleAttributePredicateList expandList=parent.right.deepCloneAndAdd(expandPredicate);
                    if (minimalityChecker.isMinimal(expandList)){
                        continue;
                    }
                    if (spCache.get(expandList).equalsFast(spCache.get(parent.right))){
                        continue;
                    }
                    queue.addLast(new LexicographicalOrderDependency(expandList,parent.right));
                }
            }

        }

        return result;
    }

    List<SingleAttributePredicate> getExpandPredicates(SingleAttributePredicateList expandSide
            ,SingleAttributePredicateList otherSide){
        boolean[] exist=new boolean[data.getColumnCount()];
        expandSide.forEach((p)->exist[p.attribute]=true);
        otherSide.forEach((p)->exist[p.attribute]=true);
        List<SingleAttributePredicate> result=new ArrayList<>();
        for (int i = 0; i < data.getColumnCount(); i++) {
            if (exist[i]){
                continue;
            }
            result.add(SingleAttributePredicate.getInstance(i, Operator.lessEqual));
            result.add(SingleAttributePredicate.getInstance(i, Operator.greaterEqual));
        }
        return result;

    }
}
