package leveretconey.cocoa.multipleStandard;

import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import leveretconey.cocoa.sample.DFSISPCacheAttachedToNode;
import leveretconey.cocoa.sample.ValidatorForG1UsingISPCache;
import leveretconey.cocoa.sample.ValidatorForG3UsingISPCache;
import leveretconey.dependencyDiscover.MinimalityChecker.ALODMinimalityChecker;
import leveretconey.cocoa.twoSideExpand.ALODTree;
import leveretconey.cocoa.twoSideExpand.ALODTree.ALODTreeNode;
import leveretconey.cocoa.twoSideExpand.TwoSideDFSSPCache;
import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Dependency.LexicographicalOrderDependency;
import leveretconey.dependencyDiscover.Discoverer.ALODDiscoverer;
import leveretconey.dependencyDiscover.MinimalityChecker.ALODMinimalityCheckerUseFD;
import leveretconey.dependencyDiscover.MinimalityChecker.LODMinimalityChecker;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicate;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicateList;
import leveretconey.dependencyDiscover.SortedPartition.SortedPartition;
import leveretconey.dependencyDiscover.Validator.ALODValidator;
import leveretconey.util.Gateway;
import leveretconey.util.Gateway.ComplexTimeGateway;
import leveretconey.util.Timer;
import leveretconey.util.Util;

public class DFSDiscovererWithMultipleStandard extends ALODDiscoverer {


    private DataFrame data;
    private LODMinimalityChecker minimalityChecker;
    private ALODTree tree;
    private Gateway traversalGateway;
    private Timer timer;
    private TwoSideDFSSPCache spCache;
    private DFSISPCacheAttachedToNode ispCache;
    private ALODValidator[] validators;
    private int odCount;
    private double[] errorRateThresholds;
    private ValidatorType[] validatorTypes;

    public static enum ValidatorType{
        G1,G3
    }

    public DFSDiscovererWithMultipleStandard() {
        this(new ValidatorType[]{ValidatorType.G1,ValidatorType.G3},new double[]{0.01,0.01});
    }

    public DFSDiscovererWithMultipleStandard(ValidatorType validator, double errorRateThreshold) {
        this(new ValidatorType[]{validator},new double[]{errorRateThreshold});
    }

    public DFSDiscovererWithMultipleStandard(ValidatorType[] validators, double[] errorRateThresholds) {
        if(validators.length!=errorRateThresholds.length){
            throw new InvalidParameterException("参数数量不匹配");
        }
        this.validatorTypes = validators;
        this.errorRateThresholds = errorRateThresholds;
    }

    private ALODValidator factory(ValidatorType type){
        switch (type){
            case G1:
                return new ValidatorForG1UsingISPCache(ispCache);
            case G3:
                return new ValidatorForG3UsingISPCache(ispCache);
        }
        throw new InvalidParameterException("input error");
    }

    @Override
    public Collection<LexicographicalOrderDependency> discover(DataFrame data, double errorRateThreshold) {
        this.data               = data;
        timer                   = new Timer();
        int maxSpCache          = (int)(2.0*1024*1024*1024/4/3/data.getTupleCount());
        spCache                 = new TwoSideDFSSPCache(data,maxSpCache);
        minimalityChecker       = new ALODMinimalityCheckerUseFD();
        traversalGateway        = new ComplexTimeGateway();
//        traversalGateway        = Gateway.AlwaysOpen;
        ispCache                = new DFSISPCacheAttachedToNode(spCache);
        tree                    = new ALODTree(data,errorRateThresholds);
        odCount                 = 0;
        validators              =new ALODValidator[validatorTypes.length];
        for (int i = 0; i < validatorTypes.length; i++) {
            validators[i]=factory(validatorTypes[i]);
        }

        Util.out(String.format("标准为%s,error rate阈值为%s",Arrays.toString(validatorTypes),Arrays.toString(errorRateThresholds)));

        for (ALODTreeNode node : tree.getLevel1NodesQueue()) {
            ispCache.updateWorkingNode(node);
            search(node);
        }
        Collection<LexicographicalOrderDependency> result = tree.getValidODs();
        if (Gateway.LogGateway.isOpen(Gateway.LogGateway.INFO)) {
            Util.out(String.format("运行结束，用时%.3fs,发现od %d个"
            , timer.getTimeUsedInSecond(), odCount));

//            int count=tree.getNodeCount(node -> {
//                if(node.states[0] instanceof ValidationResultWithAccurateBound
//                 && node.states[1] instanceof ValidationResultWithAccurateBound){
//                    return ((ValidationResultWithAccurateBound)node.states[0]).errorRate
//                            >((ValidationResultWithAccurateBound)node.states[1]).errorRate;
//                }
//                return false;
//            });
//            Util.out("g1比g3大的节点数量为"+count);
        }

        return result;
    }

    private void search(ALODTreeNode parent){
        if(!parent.willExpand()){
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
            child.validateIfNecessary(validators,data);
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
