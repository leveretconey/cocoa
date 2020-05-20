package leveretconey.cocoa.sample;

import java.util.Collection;
import java.util.Random;

import leveretconey.dependencyDiscover.MinimalityChecker.ALODMinimalityChecker;
import leveretconey.cocoa.twoSideExpand.ALODTree;
import leveretconey.cocoa.twoSideExpand.ALODTree.ALODTreeNode;
import leveretconey.cocoa.twoSideExpand.RandomSampleEstimatingALODValidator;
import leveretconey.cocoa.twoSideExpand.TwoSideDFSSPCache;
import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Dependency.LexicographicalOrderDependency;
import leveretconey.dependencyDiscover.Discoverer.ALODDiscoverer;
import leveretconey.dependencyDiscover.MinimalityChecker.LODMinimalityChecker;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicate;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicateList;
import leveretconey.dependencyDiscover.Validator.ALODValidator;
import leveretconey.dependencyDiscover.Validator.Result.ValidationResultWithBoundResultOnly;
import leveretconey.util.Gateway;
import leveretconey.util.Gateway.ComplexTimeGateway;
import leveretconey.util.Timer;
import leveretconey.util.Util;

@Deprecated
public class DFSDiscovererSampleValidateMixtureISP extends ALODDiscoverer {

    private DataFrame data;
    private double errorRateThreshold;
    private LODMinimalityChecker minimalityChecker;
    private ALODTree tree;
    private TwoSideDFSSPCache spCache;
    private DFSISPCacheAttachedToNode ispCache;
    private ALODValidator estimateValidator;
    private ALODValidator accurateValidator;
    private ExpandPredicateProvider predicateProvider;
    private ALODTreeNode workingNode;
    private AlgorithmState algorithmState;
    private double closeLowerBound,closeUpperBound;

    private Gateway traversalGateway;
    private Timer timer;
    private long mainLoopCount=0,accurateValidationCount=0;
    private long mistakeCount=0;

    @Override
    public Collection<LexicographicalOrderDependency> discover(DataFrame data, double errorRateThreshold) {
        timer                   =new Timer();
        this.data               = data;
        this.errorRateThreshold = errorRateThreshold;
        int sampleCount         =1000;
        double cautiousFactor   =4;
        closeLowerBound         =MathUtil.solveEquation(0,errorRateThreshold,
                (x) -> x + cautiousFactor * Math.sqrt( x * (1-x) / sampleCount) - errorRateThreshold);
        closeUpperBound         =MathUtil.solveEquation(errorRateThreshold,1,
                (x) -> x - cautiousFactor * Math.sqrt( x * (1-x) / sampleCount) - errorRateThreshold);
        minimalityChecker       =new ALODMinimalityChecker();
        spCache                 =new TwoSideDFSSPCache(data);
        tree                    =new ALODTree(data,errorRateThreshold);
//        traversalGateway        =Gateway.AlwaysOpen;
        traversalGateway        =new ComplexTimeGateway();
        ispCache                =new DFSISPCacheAttachedToNode(spCache);
        estimateValidator       =new RandomSampleEstimatingALODValidator(sampleCount,new Random(1));
        accurateValidator       =new ValidatorForG1UsingISPCache(ispCache);
        predicateProvider       =new ExpandPredicateProvider(data,minimalityChecker);

        for (ALODTreeNode node : tree.getLevel1NodesQueue()) {
            expandFromVeryBeginning(node);
        }

        Collection<LexicographicalOrderDependency> result = tree.getValidODs();
        if (Gateway.LogGateway.isOpen(Gateway.LogGateway.INFO)) {
            Util.out(String.format("运行结束，用时%.3fs,发现od %d个，主循环执行次数%d，精确验证次数%d,嵌入od缓存规模%d,错误次数%d"
                    , timer.getTimeUsedInSecond(), result.size(),mainLoopCount,
                    accurateValidationCount, minimalityChecker.size(),mistakeCount));
            int nodeCount                     =tree.getNodeCount((node -> true));
            int leafCount                     =tree.getNodeCount((node -> node.children==null));
            int notAccuratelyCheckedNodeCount =tree.getNodeCount((node -> node.toLOD().length()>=2
                    && node.states[0] instanceof ValidationResultWithBoundResultOnly));
            Util.out(String.format("树中共有%d个节点，其中有%d个叶子,%d个没有被验证过，比例为%f",
                    nodeCount,leafCount,notAccuratelyCheckedNodeCount,(double)notAccuratelyCheckedNodeCount/nodeCount));
        }
        return result;
    }

    private void expandFromVeryBeginning(ALODTreeNode from){
        workingNodeGoDown(from);
        algorithmState= AlgorithmState.EXPANSION;
        while (workingNode!=from.parent) {
            mainLoopCount++;
            if (Gateway.LogGateway.isOpen(Gateway.LogGateway.DEBUG) && traversalGateway.isOpen()) {
                Util.out(String.format("当前时间%.3fs,扩展到:%s,当前状态%s,主循环执行次数%d，精确验证次数%d,嵌入od缓存规模%d,错误次数%d",
                        timer.getTimeUsedInSecond(),workingNode,algorithmState,mainLoopCount, accurateValidationCount
                        ,minimalityChecker.size(),mistakeCount++));
            }
            switch (algorithmState) {
                case EXPANSION:
                    expand();
                    break;
                case VALIDATION:
                    validate();
                    break;
            }
        }
    }

    private void workingNodeGoDown(ALODTreeNode node){
        workingNode=node;
        ispCache.updateWorkingNode(workingNode);

    }
    private void workingNodeGoUp(){
        predicateProvider.removePredicateCache(workingNode);
        workingNode=workingNode.parent;
        ispCache.updateWorkingNode(workingNode);
    }

    private void validate() {
        ALODTreeNode node = workingNode;
        if (!node.states[0].isConfirmed()) {
            boolean originalExpand = node.willExpand();
            boolean originalExpandLeft = node.isExpandLeft();
            node.setStates(accurateValidator.validate(data, node));
            accurateValidationCount++;
            if ((originalExpand != node.willExpand()) || (originalExpandLeft != node.isExpandLeft())) {
//                Util.out(String.format("发现错误,它有%d个后代",tree.getNodeCount(n->n!=node && node.ancestorOf(n))));
                mistakeCount++;
                node.removeChildren();
                predicateProvider.resetPredicates(node);
            }
        }
        algorithmState = AlgorithmState.EXPANSION;
    }

    private void expand(){
        ALODTreeNode parent= workingNode;
        if (parent.changeDirectionOrWillPrune()){
            if (parent.parent.isSuspiciousToMakeMistake(closeLowerBound,closeUpperBound)) {
                    workingNodeGoUp();
                    predicateProvider.addPredicate(parent.parent, parent.nodePredicate);
                    algorithmState = AlgorithmState.VALIDATION;
                    return;
            }else if(parent.isSuspiciousToMakeMistake(closeLowerBound,closeUpperBound)){
                algorithmState= AlgorithmState.VALIDATION;
                return;
            }
        }
        SingleAttributePredicate expandPredicate=predicateProvider.getNextPredicate(parent);
        if (expandPredicate==null){
            if (parent.isNecessaryToBeCheckedAccurately()){
                algorithmState= AlgorithmState.VALIDATION;
            }else {
                parent.solidifyState();
                workingNodeGoUp();
            }
            return;
        }
        if (!parent.hasChild(expandPredicate)){
            SingleAttributePredicateList parentExpandList = parent.sideToExpand();
            SingleAttributePredicateList childExpandedList= parentExpandList.deepCloneAndAdd(expandPredicate);
            if (spCache.get(childExpandedList).equalsFast(spCache.get(parentExpandList))){
                minimalityChecker.insert(parentExpandList,expandPredicate);
                spCache.mayRemove(childExpandedList);
                predicateProvider.pollNextPredicate(parent);
                return;
            }
            parent.expand(expandPredicate);
        }
        ALODTreeNode child = parent.getChild(expandPredicate);
        if (!child.checked()) {
            child.setStates(estimateValidator.validate(data, child.toLOD()));
        }
        predicateProvider.pollNextPredicate(parent);
        workingNodeGoDown(child);
    }


    private enum AlgorithmState {
        EXPANSION, VALIDATION
    }



}
