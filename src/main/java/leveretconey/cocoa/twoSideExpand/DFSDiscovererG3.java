package leveretconey.cocoa.twoSideExpand;

import java.util.Collection;
import java.util.List;

import leveretconey.cocoa.sample.DFSISPCacheAttachedToNode;
import leveretconey.cocoa.sample.ValidatorForG3UsingISPCache;
import leveretconey.cocoa.twoSideExpand.ALODTree.ALODTreeNode;
import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Dependency.LexicographicalOrderDependency;
import leveretconey.dependencyDiscover.Discoverer.ALODDiscoverer;
import leveretconey.dependencyDiscover.MinimalityChecker.ALODMinimalityChecker;
import leveretconey.dependencyDiscover.MinimalityChecker.LODMinimalityChecker;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicate;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicateList;
import leveretconey.dependencyDiscover.SortedPartition.SortedPartition;
import leveretconey.dependencyDiscover.Validator.ALODValidator;
import leveretconey.util.Gateway;
import leveretconey.util.Gateway.ComplexTimeGateway;
import leveretconey.util.Timer;
import leveretconey.util.Util;

public class DFSDiscovererG3 extends ALODDiscoverer {


    private DataFrame data;
    private double errorRateThreshold;
    private LODMinimalityChecker minimalityChecker;
    private ALODTree tree;
    private Gateway traversalGateway;
    private Timer timer;
    private TwoSideDFSSPCache spCache;
    private DFSISPCacheAttachedToNode ispCache;
    private ALODValidator validator;
    private int odCount;

    @Override
    public Collection<LexicographicalOrderDependency> discover(DataFrame data, double errorRateThreshold) {
        this.data               = data;
        this.errorRateThreshold = errorRateThreshold;
        timer                   = new Timer();
        int maxSpCache          = (int)(2.0*1024*1024*1024/4/3/data.getTupleCount());
        spCache                 = new TwoSideDFSSPCache(data,maxSpCache);
        minimalityChecker       = new ALODMinimalityChecker();
        tree                    = new ALODTree(data,errorRateThreshold);
        traversalGateway        = new ComplexTimeGateway();
        ispCache                = new DFSISPCacheAttachedToNode(spCache);
        validator               = new ValidatorForG3UsingISPCache(ispCache);
        odCount                 = 0;

        for (ALODTreeNode node : tree.getLevel1NodesQueue()) {
            ispCache.updateWorkingNode(node);
            search(node);
        }
        Collection<LexicographicalOrderDependency> result = tree.getValidODs();
        if (Gateway.LogGateway.isOpen(Gateway.LogGateway.INFO)) {
            Util.out(String.format("运行结束，用时%.3fs,发现od %d个"
                    , timer.getTimeUsedInSecond(), odCount));
        }
        return result;
    }

    private void search(ALODTreeNode parent){

        if (!parent.willExpand()){
            return;
        }
        ispCache.updateWorkingNode(parent);
        List<SingleAttributePredicate> expandPredicates = parent.toLOD()
                .getExpandPredicates(data, minimalityChecker, parent.isExpandLeft());
        SortedPartition parentSp=spCache.get(parent.sideToExpand());
        SingleAttributePredicateList parentSideToExpand = parent.sideToExpand();
        for(int i=expandPredicates.size()-1;i>=0;i--){
            SingleAttributePredicate expandPredicate=expandPredicates.get(i);
            SingleAttributePredicateList expandedSide=parentSideToExpand.deepCloneAndAdd(expandPredicate);
            SortedPartition expandedSp=spCache.get(expandedSide);
            if (expandedSp.equalsFast(parentSp)){
                spCache.mayRemove(expandedSide);
                minimalityChecker.insert(parentSideToExpand,expandPredicate);
                continue;
            }
            ALODTreeNode child = parent.expand(expandPredicate);
            ispCache.updateWorkingNode(child);
            child.setStates(validator.validate(data,child));
            if (child.isValid()){
                odCount++;
            }
            if (Gateway.LogGateway.isOpen(Gateway.LogGateway.DEBUG) && traversalGateway.isOpen()) {
                Util.out(String.format("当前时间%.3f,扩展到:%s,发现od %d个"
                        , timer.getTimeUsedInSecond(), child, odCount));
            }

            search(child);
        }
    }
}
