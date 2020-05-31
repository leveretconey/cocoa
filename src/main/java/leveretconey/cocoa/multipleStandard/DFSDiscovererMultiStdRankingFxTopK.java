package leveretconey.cocoa.multipleStandard;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;

import javafx.util.Pair;
import leveretconey.cocoa.multipleStandard.DFSDiscovererWithMultipleStandard.ValidatorType;
import leveretconey.cocoa.ranking.LODRankingFunction;
import leveretconey.cocoa.sample.DFSISPCacheAttachedToNode;
import leveretconey.cocoa.sample.ValidatorForG1UsingISPCache;
import leveretconey.cocoa.sample.ValidatorForG3UsingISPCache;
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
import leveretconey.dependencyDiscover.SPCache.SortedPartitionCache;
import leveretconey.dependencyDiscover.SortedPartition.SortedPartition;
import leveretconey.dependencyDiscover.Validator.ALODValidator;
import leveretconey.orderap.ORDERSortedPartitionCache;
import leveretconey.util.Gateway;
import leveretconey.util.Gateway.ComplexTimeGateway;
import leveretconey.util.Timer;
import leveretconey.util.Util;

public class DFSDiscovererMultiStdRankingFxTopK extends ALODDiscoverer {


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
    private int k=20;
    private LODRankingFunction rankingFunction;
    private PriorityQueue<Pair<Double,LexicographicalOrderDependency>> ods=
            new PriorityQueue<>((p1,p2)->getSign(p2.getKey(),p1.getKey()));
    private int maxODLength=4;

    private int getSign(double d1,double d2){
        double diff=d1-d2;
        if (diff<0)
            return -1;
        if (diff>0)
            return 1;
        return 0;
    }

    public DFSDiscovererMultiStdRankingFxTopK() {
        this(new ValidatorType[]{ValidatorType.G1},new double[]{0.01});
    }

    public DFSDiscovererMultiStdRankingFxTopK(ValidatorType validator, double errorRateThreshold) {
        this(new ValidatorType[]{validator},new double[]{errorRateThreshold});
    }
    public DFSDiscovererMultiStdRankingFxTopK(ValidatorType validator,
                                              double errorRateThreshold,int k,LODRankingFunction rankingFunction,int maxODLength) {
        this(new ValidatorType[]{validator},new double[]{errorRateThreshold},k,rankingFunction,maxODLength);
    }

    public DFSDiscovererMultiStdRankingFxTopK(ValidatorType[] validators, double[] errorRateThresholds) {
        if(validators.length!=errorRateThresholds.length){
            throw new InvalidParameterException("参数数量不匹配");
        }
        this.validatorTypes = validators;
        this.errorRateThresholds = errorRateThresholds;
    }

    public DFSDiscovererMultiStdRankingFxTopK(ValidatorType[] validators, double[] errorRateThresholds
                                            , int k, LODRankingFunction rankingFunction,int maxODLength) {
        if(validators.length!=errorRateThresholds.length){
            throw new InvalidParameterException("参数数量不匹配");
        }
        this.validatorTypes = validators;
        this.errorRateThresholds = errorRateThresholds;
        this.k=k;
        this.rankingFunction=rankingFunction;
        this.maxODLength=maxODLength;
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
        if (Gateway.LogGateway.isOpen(Gateway.LogGateway.INFO)) {
            Util.out(String.format("运行结束，用时%.3fs,发现od %d个"
            , timer.getTimeUsedInSecond(), odCount));
        }

        List<LexicographicalOrderDependency> result=new ArrayList<>();
        for (int i = 0; i < k; i++) {
            if (ods.isEmpty()){
                break;
            }
            Pair<Double, LexicographicalOrderDependency> pair = ods.poll();
            Util.out(pair);
            result.add(pair.getValue());
        }

        if (Gateway.LogGateway.isOpen(Gateway.LogGateway.INFO)) {
            Util.out(String.format("运行结束，用时%.3fs,发现od %d个"
                    , timer.getTimeUsedInSecond(), result.size()));
        }
        return result;
    }

    private void search(ALODTreeNode parent){
        if(!parent.willExpand() || parent.toLOD().length()>maxODLength){
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
                ods.add(new Pair<>(rankingFunction.getRanking(child.toLOD()
                        ,ispCache.getISP(child)),child.toLOD()
                        ));
            }
            if (Gateway.LogGateway.isOpen(Gateway.LogGateway.DEBUG) && traversalGateway.isOpen()) {
                Util.out(String.format("当前时间%.3f,扩展到:%s,发现od %d个"
                    , timer.getTimeUsedInSecond(), child, odCount));
            }
            search(child);
        }
    }
}
