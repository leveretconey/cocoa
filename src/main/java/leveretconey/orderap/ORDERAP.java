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
import leveretconey.util.Gateway;
import leveretconey.util.Timer;
import leveretconey.util.Util;

import static leveretconey.cocoa.multipleStandard.DFSDiscovererWithMultipleStandard.*;

public class ORDERAP extends ALODDiscoverer {

    private ValidatorType type= ValidatorType.G1;
    private DataFrame data;
    LODMinimalityChecker minimalityChecker;
    Timer timer;
    @Override
    public Collection<LexicographicalOrderDependency> discover(DataFrame data, double errorRateThreshold) {
        this.data = data;
        List<LexicographicalOrderDependency> result=new ArrayList<>();
        SortedPartitionCache spCache=new ORDERSortedPartitionCache(data);
        LinkedList<LexicographicalOrderDependency> queue=new LinkedList<>();

        for (int lhs = 1; lhs <= data.getColumnCount(); lhs++) {
            for (int rhs = 1; rhs <= data.getColumnCount(); rhs++) {
                if (lhs==rhs){
                    continue;
                }
                queue.addLast(LexicographicalOrderDependency.fromString(String.format("%d<=->%d<=",lhs,rhs)));
                queue.addLast(LexicographicalOrderDependency.fromString(String.format("%d>=->%d<=",lhs,rhs)));
            }
        }

        minimalityChecker=new ALODMinimalityCheckerUseFD();
        timer=new Timer();
        Gateway traversalGateway=new Gateway.ComplexTimeGateway();
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
            if (Gateway.LogGateway.isOpen(Gateway.LogGateway.DEBUG) && traversalGateway.isOpen()) {
                Util.out(String.format("当前时间%.3f,扩展到:%s,发现od %d个"
                        , timer.getTimeUsedInSecond(), parent, result.size()));
            }
//            if (parent.left.get(0)==parent.right.get(0)){
//                int x=1;
//            }
            if (errorRate>errorRateThreshold){
                for (SingleAttributePredicate expandPredicate : getExpandPredicates(parent.left,parent.right)) {
                    SingleAttributePredicateList expandList=parent.left.deepCloneAndAdd(expandPredicate);
                    if (!minimalityChecker.isMinimal(expandList)){
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
                    if (!minimalityChecker.isMinimal(expandList)){
                        continue;
                    }
                    if (spCache.get(expandList).equalsFast(spCache.get(parent.right))){
                        continue;
                    }
                    queue.addLast(new LexicographicalOrderDependency(parent.left,expandList));
                }
            }

        }
        Util.out(String.format("运行结束，用时%.3fs,发现od %d个"
                , timer.getTimeUsedInSecond(), result.size()));

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
