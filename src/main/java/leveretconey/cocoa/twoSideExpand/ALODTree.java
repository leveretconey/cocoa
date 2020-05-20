package leveretconey.cocoa.twoSideExpand;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;

import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Dependency.LexicographicalOrderDependency;
import leveretconey.dependencyDiscover.Predicate.Operator;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicate;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicateList;
import leveretconey.dependencyDiscover.Validator.ALODValidator;
import leveretconey.dependencyDiscover.Validator.Result.ValidationResultWithAccurateBound;
import leveretconey.dependencyDiscover.Validator.Result.ValidationResultWithBound;
import leveretconey.dependencyDiscover.Validator.Result.ValidationResultWithBoundResultOnly;

public class ALODTree {
    private int countAttribute;
    private ALODTreeNode root;
    private double[] errorRateThresholds;

    public ALODTreeNode getRoot() {
        return root;
    }

    public ALODTree(DataFrame data, double ...errorRateThresholds) {
        this.errorRateThresholds = errorRateThresholds;
        this.countAttribute=data.getColumnCount();

        root=new ALODTreeNode(null,null);
        root.states=new ValidationResultWithBound[errorRateThresholds.length];
        Arrays.fill(root.states,new ValidationResultWithBoundResultOnly(true,true,false,false));
        for (int right = 0; right < countAttribute; right++) {
            ALODTreeNode level1Node = new ALODTreeNode(root,
                    SingleAttributePredicate.getInstance(right, Operator.lessEqual));
            level1Node.states=new ValidationResultWithBound[errorRateThresholds.length];
            Arrays.fill(level1Node.states,new ValidationResultWithBoundResultOnly(false,true,false,false));
            for (int left = 0; left < countAttribute; left++) {
                if (right==left){
                    continue;
                }
                ALODTreeNode level2Node = new ALODTreeNode(level1Node,
                        SingleAttributePredicate.getInstance(left, Operator.lessEqual));
                level2Node = new ALODTreeNode(level1Node,
                        SingleAttributePredicate.getInstance(left, Operator.greaterEqual));
            }
        }
    }

    public int getNodeCount(ALODTreeNode from,Predicate<ALODTreeNode> predicate){
        LinkedList<ALODTreeNode> queue=new LinkedList<>();
        queue.addLast(from);
        int result=0;
        while (!queue.isEmpty()){
            ALODTreeNode node = queue.pollLast();
            if (predicate.test(node)){
                result++;
            }
            if (node.children!=null){
                for (ALODTreeNode child : node.children) {
                    if (child!=null){
                        queue.addLast(child);
                    }
                }
            }
        }
        return result;
    }
    public int getNodeCount(Predicate<ALODTreeNode> predicate){
        return getNodeCount(root,predicate);
    }

    public ALODTree(DataFrame data) {
        this(data,0.01);

    }

    public Collection<LexicographicalOrderDependency> getValidODs(){
        LinkedList<ALODTreeNode> queue = getLevel2NodesQueue();
        List<LexicographicalOrderDependency> result=new ArrayList<>();
        while (!queue.isEmpty()){
            ALODTreeNode node = queue.pollFirst();
            if (node.checked() && node.isValid()){
                result.add(node.toLOD());
            }
            if (node.children==null)
                continue;
            for (ALODTreeNode child : node.children) {
                if (child==null)
                    continue;
                queue.addLast(child);
            }
        }
        return result;
    }

    public LinkedList<ALODTreeNode> getLevel1NodesQueue(){
        LinkedList<ALODTreeNode> queue=new LinkedList<>();
        for (ALODTreeNode level1Node : root.children) {
            if (level1Node==null){
                continue;
            }
            queue.addLast(level1Node);
        }
        return queue;
    }
    public LinkedList<ALODTreeNode> getLevel2NodesQueue(){
        LinkedList<ALODTreeNode> queue=new LinkedList<>();
        for (ALODTreeNode level1Node : root.children) {
            if (level1Node==null){
                continue;
            }
            for (ALODTreeNode level2Node : level1Node.children) {
                if (level2Node!=null){
                    queue.addLast(level2Node);
                }
            }
        }
        return queue;
    }

    public class ALODTreeNode{
        public ALODTreeNode parent;
        public ValidationResultWithBound[] states;
        public SingleAttributePredicate nodePredicate;
        public ALODTreeNode[] children;
        private LexicographicalOrderDependency dependency;


        public boolean ancestorOf(ALODTreeNode descendant){
            int lengthDiff=descendant.toLOD().length()-toLOD().length();
            if (lengthDiff<0){
                return false;
            }
            for (int i = 0; i < lengthDiff; i++) {
                descendant=descendant.parent;
                if (descendant==null){
                    return false;
                }
            }
            return descendant==this;
        }

        public boolean checked(){
            return states!=null;
        }

        public boolean isValid(){
            for (int i = 0; i < states.length; i++) {
                if (!states[i].isValid(errorRateThresholds[i])){
                    return false;
                }
            }
            return true;
        }


        public ALODTreeNode getChild(SingleAttributePredicate predicate){
            if (children==null){
                return null;
            }
            return children[predicate2Index(predicate)];
        }

        public boolean willExpand(){
            for (int i = 0; i < states.length; i++) {
                if (states[i].lowerBoundGreaterThan(errorRateThresholds[i])){
                    return false;
                }
            }
            if (isExpandLeft()){
                return true;
            }
            for (int i = 0; i < states.length; i++) {
                if (!states[i].upperBoundLessThan(errorRateThresholds[i])){
                    return true;
                }
            }
            return false;
        }

        public SingleAttributePredicateList expandedSide(){
            return parent.isExpandLeft()?toLOD().left:toLOD().right;
        }

        public SingleAttributePredicateList sideToExpand(){
            return isExpandLeft()?toLOD().left:toLOD().right;
        }

        public boolean isChangeDirection(){
            return parent.isExpandLeft()!=isExpandLeft();
        }

        @Deprecated
        public boolean isNecessaryToBeCheckedAccurately(){
            ValidationResultWithBound state = states[0];
            if (state.isConfirmed()){
                return false;
            }
            boolean  b1=state.isValid(errorRateThresholds[0])
                    ,b2=state.lowerBoundGreaterThan(errorRateThresholds[0])
                    ,b3=state.upperBoundLessThan(errorRateThresholds[0]);
            return !hasQualifiedChild((node)->node.states[0].isValid(errorRateThresholds[0])==b1) ||
                   !hasQualifiedChild((node)->node.states[0].lowerBoundGreaterThan(errorRateThresholds[0])==b2) ||
                   !hasQualifiedChild((node)->node.states[0].upperBoundLessThan(errorRateThresholds[0])==b3);
        }

        public boolean isAccessible(){
            ALODTreeNode node=this;
            while ( node.parent!=null ){
                node=node.parent;
            }
            return node==root;
        }

        public void validateIfNecessary(ALODValidator[] validators,DataFrame data){
            states = new ValidationResultWithBound[validators.length];
            if (toLOD().length()>2 && parent.isExpandLeft()){
                for (int i = 0; i < validators.length; i++) {
                    ValidationResultWithBound parentState = parent.states[i];
                    if (parentState.lowerBoundGreaterThan(errorRateThresholds[i]) ||
                        parentState.upperBoundLessThan(errorRateThresholds[i]) ||
                        parentState.isValid(errorRateThresholds[i])){
                        states[i]=new ValidationResultWithBoundResultOnly(true,false
                                ,parentState.lowerBoundGreaterThan(errorRateThresholds[i])
                                ,parentState.upperBoundLessThan(errorRateThresholds[i]));
                    }else {
                        states[i]=validators[i].validate(data,this);
                    }
                }
                if (willExpand() && isExpandLeft()){
                    for (int i = 0; i < validators.length; i++) {
                        if (states[i].upperBoundLessThan(errorRateThresholds[i]) ||
                            states[i].lowerBoundGreaterThan(errorRateThresholds[i])){
                            continue;
                        }
                        if (states[i] instanceof ValidationResultWithAccurateBound) {
                            continue;
                        }
                        states[i]=validators[i].validate(data,this);
                    }
                }

            }else {
                for (int i = 0; i < validators.length; i++) {
                    states[i]=validators[i].validate(data,this);
                }
            }
        }

        @Deprecated
        public boolean hasQualifiedChild(Predicate<ALODTreeNode> predicate){
            if (children==null){
                return false;
            }
            for (ALODTreeNode child : children) {
                if (child!=null && predicate.test(child)){
                    return true;
                }
            }
            return false;
        }

        public ALODTreeNode(ALODTreeNode parent,SingleAttributePredicate predicate){
            if (parent!=null){
                if (parent.children==null){
                    parent.children=new ALODTreeNode[2*countAttribute];
                }
                parent.children[predicate2Index(predicate)]=this;
            }
            nodePredicate=predicate;
            this.parent=parent;
        }

        public boolean hasChild(SingleAttributePredicate predicate){
            return children!=null && children[predicate2Index(predicate)]!=null;
        }



        public void setStates(ValidationResultWithBound ...states) {
            this.states = states;
        }

        public ALODTreeNode expand(SingleAttributePredicate predicate){
            if (children!=null){
                int index=predicate2Index(predicate);
                if (children[index]!=null){
                    return children[index];
                }
            }
            return new ALODTreeNode(this, predicate);
        }

        public boolean changeDirectionOrWillPrune(){
            return isChangeDirection() || !willExpand();
        }

        public LexicographicalOrderDependency toLOD(){
            if (dependency==null){
                ALODTreeNode node=this;
                dependency=new LexicographicalOrderDependency();
                while (node.parent!=null){
                    if (node.parent.isExpandLeft()) {
                        dependency.left.add(node.nodePredicate);
                    } else {
                        dependency.right.add(node.nodePredicate);
                    }
                    node=node.parent;
                }
                Collections.reverse(dependency.left.list);
                Collections.reverse(dependency.right.list);
            }
            return dependency;
        }

        public void removeChildren(){
            if (children==null){
                return;
            }
            for (ALODTreeNode child : children) {
                if (child != null) {
                    child.parent = null;
                }
            }
            children=null;
        }

        public boolean isExpandLeft(){
            return !isValid();
        }

        @Deprecated
        public void solidifyState(){
            ValidationResultWithBound state = states[0];
            if (state.isConfirmed()){
                return;
            }
            state=new ValidationResultWithBoundResultOnly(state.isValid(errorRateThresholds[0]),
                    true,state.lowerBoundGreaterThan(errorRateThresholds[0]),state.upperBoundLessThan(errorRateThresholds[0]));
        }

        @Deprecated
        public boolean isSuspiciousToMakeMistake(double closeLowerBound, double closeUpperBound){
            ValidationResultWithBound state = states[0];
            if (state.isConfirmed()){
                return false;
            }
            if (! (state instanceof ValidationResultWithAccurateBound)){
                return false;
            }
            ValidationResultWithAccurateBound resultWithAccurateBound = (ValidationResultWithAccurateBound) state;

            return      (resultWithAccurateBound.errorRate           > closeLowerBound && resultWithAccurateBound.errorRate           < closeUpperBound)
                    ||  (resultWithAccurateBound.errorRateLowerBound > closeLowerBound && resultWithAccurateBound.errorRateLowerBound < closeUpperBound)
                    ||  (resultWithAccurateBound.errorRateUpperBound > closeLowerBound && resultWithAccurateBound.errorRateUpperBound < closeUpperBound);

        }

        @Override
        public String toString() {
            return String.format("%s %s",toLOD(), Arrays.toString(states));
        }
    }

    private int predicate2Index(SingleAttributePredicate predicate){
        int attribute=predicate.attribute;
        if (predicate.operator== Operator.greaterEqual)
            attribute+=countAttribute;
        return attribute;
    }
    private SingleAttributePredicate index2Predicate(int index){
        if (index<countAttribute){
            return SingleAttributePredicate.getInstance(index,Operator.lessEqual);
        }else {
            return SingleAttributePredicate.getInstance(index-countAttribute,Operator.greaterEqual);
        }
    }
}
