package leveretconey.dependencyDiscover.Dependency;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;


import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.MinimalityChecker.LODMinimalityChecker;
import leveretconey.dependencyDiscover.Predicate.Operator;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicate;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicateList;
import leveretconey.util.Util;

/*
    ALOD的谓词只需要考虑大于等于和小于等于
 */
public class LexicographicalOrderDependency implements AbstractDependency {
    public SingleAttributePredicateList left=new SingleAttributePredicateList();
    public SingleAttributePredicateList right=new SingleAttributePredicateList();

    @Override
    public String toString() {
        return left+"->"+right;
    }

    public LexicographicalOrderDependency(SingleAttributePredicateList left, SingleAttributePredicateList right) {
        this.left = left;
        this.right = right;
    }

    public LexicographicalOrderDependency(SingleAttributePredicate left,SingleAttributePredicate right){
        this.left.add(left);
        this.right.add(right);
    }

    public LexicographicalOrderDependency() {
    }
    public boolean expand(SingleAttributePredicate predicate,boolean expandLeft){
        if(expandLeft)
            return left.add(predicate);
        else
            return right.add(predicate);
    }
    public static LexicographicalOrderDependency fromString(String s){
        String[] parts=s.split("->");
        LexicographicalOrderDependency od=new LexicographicalOrderDependency();
        od.left=SingleAttributePredicateList.fromString(parts[0]);
        od.right=SingleAttributePredicateList.fromString(parts[1]);
        return od;

    }


    public List<SingleAttributePredicate> getExpandPredicates(DataFrame data) {
        Set<Integer> attributes = new HashSet<>();
        for (SingleAttributePredicate predicate : left) {
            attributes.add(predicate.attribute);
        }
        for (SingleAttributePredicate predicate : right) {
            attributes.add(predicate.attribute);
        }
        List<SingleAttributePredicate> result = new ArrayList<>();
        for(int attribute=data.getColumnCount()-1;attribute>=0;attribute--){
            if (attributes.contains(attribute)) {
                continue;
            }
            result.add(SingleAttributePredicate.getInstance(attribute, Operator.greaterEqual));
            result.add(SingleAttributePredicate.getInstance(attribute, Operator.lessEqual));
        }
        return result;
    }

    //加一个标准,也就是还检查了最小性
    public List<SingleAttributePredicate> getExpandPredicates
            (DataFrame data, LODMinimalityChecker minimalityChecker,boolean expandleft) {
        List<SingleAttributePredicate> expandPredicates
                = getExpandPredicates(data);
        SingleAttributePredicateList list = expandleft ? left : right;
        expandPredicates.removeIf
                ((predicate)->!minimalityChecker.isMinimal(list,predicate));
        return expandPredicates;
    }

    public static Comparator<LexicographicalOrderDependency> lengthComparator=
            (od1,od2)->od1.length()-od2.length();

    public boolean violate(DataFrame data,int tuple1,int tuple2){
        return !left.violate(data,tuple1,tuple2) && right.violate(data,tuple1,tuple2);
    }


    public static LexicographicalOrderDependency random(int countColumn,int odLength){
        Random random=new Random();
        List<Integer> attributes = Util.range(countColumn);
        Collections.shuffle(attributes,random);
        int countAttributeLeft=random.nextInt(odLength-1)+1;
        LexicographicalOrderDependency od=new LexicographicalOrderDependency();
        int rangePointer;
        for(rangePointer=0;rangePointer<countAttributeLeft;rangePointer++){
            SingleAttributePredicate predicate=SingleAttributePredicate.getInstance
                    (attributes.get(attributes.get(rangePointer))
                    ,random.nextBoolean()?Operator.lessEqual:Operator.greaterEqual);
            od.left.add(predicate);
        }
        for (;rangePointer<odLength;rangePointer++){
            SingleAttributePredicate predicate=SingleAttributePredicate.getInstance
                    (attributes.get(attributes.get(rangePointer))
                    ,random.nextBoolean()?Operator.lessEqual:Operator.greaterEqual);
            od.right.add(predicate);
        }
        return od;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LexicographicalOrderDependency)) return false;
        LexicographicalOrderDependency that = (LexicographicalOrderDependency) o;
        return this.left.equals(that.left) && this.right.equals(that.right);
    }

    @Override
    public int hashCode() {
        return 961+left.hashCode() * 31 + right.hashCode();
    }

    public int length(){
        return left.size()+right.size();
    }

    public boolean isPrefixOf(LexicographicalOrderDependency other){
        return left.isPrefixOf(other.left) && right.isPrefixOf(other.right);
    }

    public LexicographicalOrderDependency deepClone(){
        LexicographicalOrderDependency result=new LexicographicalOrderDependency();
        result.left=new SingleAttributePredicateList(left);
        result.right=new SingleAttributePredicateList(right);
        return result;
    }

    public LexicographicalOrderDependency deepCloneAndExpand(SingleAttributePredicate predicate,boolean expandLeft){
        LexicographicalOrderDependency result=deepClone();
        if (expandLeft){
            result.left.add(predicate);
        }else {
            result.right.add(predicate);
        }
        return result;
    }

    public SingleAttributePredicateList getList(boolean leftHand){
        if (leftHand){
            return left;
        }else {
            return right;
        }
    }
}



