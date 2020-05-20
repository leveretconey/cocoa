package leveretconey.dependencyDiscover.SortedPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import leveretconey.dependencyDiscover.Validator.Result.ALODValidationResultWithBound;
import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Dependency.LexicographicalOrderDependency;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicate;
import leveretconey.dependencyDiscover.Validator.Result.LODValidationResult;
import leveretconey.dependencyDiscover.Validator.Result.ValidationResultWithAccurateBound;
import leveretconey.dependencyDiscover.Validator.Result.ValidationResultWithAccurateErrorRate;
import leveretconey.util.TimeStatistics;
import leveretconey.util.Util;

public class TwoSideSortedPartition {
    public SortedPartition left;
    public SortedPartition right;

    protected TwoSideSortedPartition() {
    }

    public LODValidationResult validateForLOD(){
        LODValidationResult result=new LODValidationResult();
        result.status= LODValidationResult.LODStatus.VALID;

        int[] row2RightGroupIndex= right.index2groupIndex;

        int max=0,min=0,maxvalue=0,minvalue=0;
        int maxLast=0,maxLastValue=0;
        for(int beginPointer = 0; beginPointer< left.begins.size()-1; beginPointer++){
            int groupBegin= left.begins.get(beginPointer);
            int groupEnd= left.begins.get(beginPointer+1);
            max=min= left.indexes[groupBegin];
            maxvalue=minvalue=row2RightGroupIndex[max];
            for(int i=groupBegin+1;i<groupEnd;i++){
                int index= left.indexes[i];
                int value=row2RightGroupIndex[index];
                if(value<minvalue){
                    min=index;
                    minvalue=value;
                }
                if(maxvalue<value){
                    max=index;
                    maxvalue=value;
                }
            }

            if(result.status== LODValidationResult.LODStatus.VALID && max!=min){
                result.status= LODValidationResult.LODStatus.SPLIT;
                result.violationRows.add(min);
                result.violationRows.add(max);
            }
            if(beginPointer>=1 && maxLastValue>minvalue){
                result.status= LODValidationResult.LODStatus.SWAP;
                result.violationRows.clear();
                result.violationRows.add(min);
                result.violationRows.add(maxLast);
                break;
            }
            maxLast=max;
            maxLastValue=maxvalue;
        }
        return result;
    }

    public LODValidationResult validateForFullViolation(){
        LODValidationResult result=new LODValidationResult(LODValidationResult.LODStatus.VALID);
        Set<Integer> resultSet=new HashSet<>();
        int[] row2RightGroupIndex= right.index2groupIndex;
        boolean resultSplit=false;
        for(int beginPointer = 0; beginPointer< left.begins.size()-1; beginPointer++) {
            int groupBegin = left.begins.get(beginPointer);
            int groupEnd = left.begins.get(beginPointer + 1);
            int value =row2RightGroupIndex[left.indexes[groupBegin]];
            boolean split=false;
            for(int i=groupBegin+1;i<groupEnd;i++){
                if(row2RightGroupIndex[left.indexes[i]]!=value){
                    split=true;
                    resultSplit=true;
                    break;
                }
            }

            if(split){
                for(int i=groupBegin;i<groupEnd;i++){
                    resultSet.add(left.indexes[i]);
                }
            }
        }

        boolean resultSwap=false;
        TreeSet<Integer> previousValues=new TreeSet<>();
        for(int beginPointer = 0; beginPointer< left.begins.size()-1; beginPointer++) {
            int groupBegin = left.begins.get(beginPointer);
            int groupEnd = left.begins.get(beginPointer + 1);
            for(int i=groupBegin;i<groupEnd;i++){
                if(previousValues.ceiling(1+row2RightGroupIndex[left.indexes[i]])!=null){
                    resultSet.add(left.indexes[i]);
                    resultSwap=true;
                }
            }
            for(int i=groupBegin;i<groupEnd;i++){
                previousValues.add(row2RightGroupIndex[left.indexes[i]]);
            }
        }

        TreeSet<Integer> backwardValues=new TreeSet<>();
        for(int beginPointer = left.begins.size()-2; beginPointer>=0; beginPointer--) {
            int groupBegin = left.begins.get(beginPointer);
            int groupEnd = left.begins.get(beginPointer + 1);
            for(int i=groupBegin;i<groupEnd;i++){
                if(backwardValues.floor(row2RightGroupIndex[left.indexes[i]]-1)!=null){
                    resultSet.add(left.indexes[i]);
                    resultSwap=true;
                }
            }
            for(int i=groupBegin;i<groupEnd;i++){
                backwardValues.add(row2RightGroupIndex[left.indexes[i]]);
            }
        }

        if(resultSwap){
            result.status= LODValidationResult.LODStatus.SWAP;
        }else if(resultSplit){
            result.status= LODValidationResult.LODStatus.SPLIT;
        }
        result.violationRows.addAll(resultSet);
        return result;
    }

    public ALODValidationResultWithBound validateForALODWithG1(){
        TimeStatistics.TimeStopper timer = TimeStatistics.start("g1精确计算");
        int tupleCount= left.indexes.length;
        long swapViolationCount=0,splitViolationCount=0,neverViolationCount=0;
        int[] row2RightGroupIndex= right.index2groupIndex;
        SegmentTreeForG1 tree=new SegmentTreeForG1(0, right.begins.size()-2);
        int[] globalInt2Count=new int[right.begins.size()-1];

        for(int beginPointer = 0; beginPointer< left.begins.size()-1; beginPointer++) {

            int groupBegin = left.begins.get(beginPointer);
            int groupEnd = left.begins.get(beginPointer + 1);
            int groupLength=groupEnd-groupBegin;
            HashMap<Integer,Integer> groupInt2count=new HashMap<>();

            for (int i = groupBegin ; i < groupEnd; i++) {
                int rightGroupIndex=row2RightGroupIndex[left.indexes[i]];
                int swapIncrease=tree.query(rightGroupIndex+1,Integer.MAX_VALUE);
                swapViolationCount += swapIncrease;
                neverViolationCount += (groupBegin-swapIncrease-globalInt2Count[rightGroupIndex]);
                groupInt2count.put(rightGroupIndex,groupInt2count.getOrDefault(rightGroupIndex,0)+1);
            }

            for (int i = groupBegin ; i < groupEnd; i++) {
                int rightGroupIndex=row2RightGroupIndex[left.indexes[i]];
                tree.insert(rightGroupIndex);
                globalInt2Count[rightGroupIndex]++;
            }
            long groupSplitViolationCount=0;
            for (int count : groupInt2count.values()) {
                groupSplitViolationCount+=(long)count * (groupLength-count);
            }
            splitViolationCount+=groupSplitViolationCount/2;
        }
        long totalTuplePairCount=(long)tupleCount*(tupleCount-1);
        timer.stop();
        return new ALODValidationResultWithBound(splitViolationCount,swapViolationCount,
                neverViolationCount,totalTuplePairCount,true);
    }


    public ValidationResultWithAccurateBound validateForALODWithG3(){
        SortedPartition combineSP=left.deepClone();
        combineSP.intersect(right);

        TimeStatistics.TimeStopper timer = TimeStatistics.start("g3精确计算");
        SegmentTreeForG3 segmentTree=new SegmentTreeForG3(0,right.getGroupCount()-1);
        //misl = max increasing subsequence length
        int erMISL=0,lbMISL=0,ubMISL=0;
        int lastIndexInLeft=-1;
        List<int[]> insertGroups=new ArrayList<>();
        for(int beginPointer = 0; beginPointer< combineSP.begins.size()-1; beginPointer++) {
            int groupBegin       = combineSP.begins.get(beginPointer);
            int groupEnd         = combineSP.begins.get(beginPointer + 1);
            int groupLength      = groupEnd - groupBegin;
            int beginTupleIndex  = combineSP.indexes[groupBegin];
            int indexInLeft      = left.index2groupIndex[beginTupleIndex];
            int indexInRight     = right.index2groupIndex[beginTupleIndex];

            if (indexInLeft != lastIndexInLeft){
                for (int[] insertGroup : insertGroups) {
                    segmentTree.insert(insertGroup[0],insertGroup[1],SegmentTreeForG3.Option.ERROR_RATE);
                    segmentTree.insert(insertGroup[0],insertGroup[2],SegmentTreeForG3.Option.LOWER_BOUND);
                }
                insertGroups.clear();
            }

            int erLength = groupLength + segmentTree.query(0,indexInRight, SegmentTreeForG3.Option.ERROR_RATE);
            erMISL=Math.max(erMISL,erLength);

            int lbLength = 1 + segmentTree.query(0,indexInRight-1, SegmentTreeForG3.Option.LOWER_BOUND);
            lbMISL=Math.max(lbLength,lbMISL);

            int ubLength = groupLength + segmentTree.query(0,indexInRight, SegmentTreeForG3.Option.UPPER_BOUND);
            ubMISL=Math.max(ubMISL,ubLength);
            segmentTree.insert(indexInRight,ubLength, SegmentTreeForG3.Option.UPPER_BOUND);

            insertGroups.add(new int[]{indexInRight,erLength,lbLength});
            lastIndexInLeft=indexInLeft;
        }
        double tupleCount=left.getTupleCount();
        double er=1-erMISL/tupleCount,erlb=1-ubMISL/tupleCount,erub=1-lbMISL/tupleCount;
        timer.stop();
        return new ValidationResultWithAccurateBound(er,true,erlb,erub);
    }

//    @Deprecated
//    public ValidationResultWithAccurateErrorRate validateForALODWithG3Backup(){
//        SortedPartition combineSP=left.deepClone();
//        combineSP.intersect(right);
//
//        List<G3DPGroup> dp=new ArrayList<>();
//        for(int beginPointer = 0; beginPointer< combineSP.begins.size()-1; beginPointer++) {
//            int groupBegin = combineSP.begins.get(beginPointer);
//            int groupEnd = combineSP.begins.get(beginPointer + 1);
//            int groupLength = groupEnd - groupBegin;
//            int beginTupleIndex=combineSP.indexes[groupBegin];
//            dp.add(new G3DPGroup(groupLength,left.index2groupIndex[beginTupleIndex]
//                    , right.index2groupIndex[beginTupleIndex]));
//        }
//
//        SegmentTreeForG3 erTree=new SegmentTreeForG3(0,right.getGroupCount());
//        int erISL=0;
//        int lastLeftGroupBegin=0;
//        int dpSize=dp.size();
//        for (int i = 0; i < dpSize; i++) {
//            G3DPGroup currentGroup=dp.get(i);
//            if (i!=0 && currentGroup.indexInLeft!=dp.get(i-1).indexInLeft){
//                for(int j=lastLeftGroupBegin;j<i;j++){
//                    G3DPGroup group = dp.get(j);
//                    erTree.insert(group.indexInRight,group.maxLength);
//                }
//                lastLeftGroupBegin=i;
//            }
//            currentGroup.maxLength=currentGroup.tupleCount+erTree.query(0,currentGroup.indexInRight);
//            erISL = Math.max(erISL, currentGroup.maxLength);
//        }
//        return new ValidationResultWithAccurateErrorRate(
//                1-(double)erISL/left.indexes.length);
//    }
//
//    @Deprecated
//    public ValidationResultWithAccurateErrorRate validateForALODWithG3NSquare(){
//        SortedPartition combineSP=left.deepClone();
//        combineSP.intersect(right);
//
//        List<G3DPGroup> dp=new ArrayList<>();
//        for(int beginPointer = 0; beginPointer< combineSP.begins.size()-1; beginPointer++) {
//
//            int groupBegin = combineSP.begins.get(beginPointer);
//            int groupEnd = combineSP.begins.get(beginPointer + 1);
//            int groupLength = groupEnd - groupBegin;
//            int beginTupleIndex=combineSP.indexes[groupBegin];
//            dp.add(new G3DPGroup(groupLength,left.index2groupIndex[beginTupleIndex]
//                    , right.index2groupIndex[beginTupleIndex]));
//        }
//
//        int longestIncreasingSubsequenceLength=0;
//        for (G3DPGroup currentGroup : dp) {
//            for (int j = 0; ; j++) {
//                G3DPGroup previousGroup = dp.get(j);
//                if (previousGroup.indexInLeft == currentGroup.indexInLeft) {
//                    break;
//                }
//                if (currentGroup.indexInRight >= previousGroup.indexInRight
//                        && previousGroup.maxLength > currentGroup.maxLength) {
//                    currentGroup.maxLength = previousGroup.maxLength;
//                }
//            }
//            currentGroup.maxLength += currentGroup.tupleCount;
//            longestIncreasingSubsequenceLength = Math.max(longestIncreasingSubsequenceLength, currentGroup.maxLength);
//        }
//        return new ValidationResultWithAccurateErrorRate(
//                1-(double)longestIncreasingSubsequenceLength/left.indexes.length);
//    }
//    private static class G3DPGroup{
//        int tupleCount;
//        int indexInLeft;
//        int indexInRight;
//        int maxLength=0;
//
//        G3DPGroup(int tupleCount, int indexInLeft, int indexInRight) {
//            this.tupleCount = tupleCount;
//            this.indexInLeft = indexInLeft;
//            this.indexInRight = indexInRight;
//        }
//
//        @Override
//        public String toString() {
//            return "G3DPGroup{" +
//                    "tupleCount=" + tupleCount +
//                    ", indexInLeft=" + indexInLeft +
//                    ", indexInRight=" + indexInRight +
//                    ", maxLength=" + maxLength +
//                    '}';
//        }
//    }

    public TwoSideSortedPartition(DataFrame data) {
        left =new SortedPartition(data);
        right =new SortedPartition(data);
    }

    public TwoSideSortedPartition(SortedPartition left, SortedPartition right) {
        this.left = left;
        this.right = right;
    }

    public TwoSideSortedPartition(DataFrame data, LexicographicalOrderDependency od){
        left =new SortedPartition(data);
        for (SingleAttributePredicate predicate : od.left.list) {
            left.intersect(data,predicate);
        }
        right =new SortedPartition(data);
        for (SingleAttributePredicate predicate : od.right.list) {
            right.intersect(data,predicate);
        }
    }

    public TwoSideSortedPartition deepClone(){
        return new TwoSideSortedPartition(left.deepClone(), right.deepClone());
    }

    public void intersect(DataFrame data,SingleAttributePredicate predicate,boolean intersectLeft){
        if (intersectLeft){
            left.intersect(data, predicate);
        }else {
            right.intersect(data, predicate);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TwoSideSortedPartition)) return false;
        TwoSideSortedPartition that = (TwoSideSortedPartition) o;
        return Objects.equals(left, that.left) &&
                Objects.equals(right, that.right) ;
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right);
    }

    @Override
    public String toString() {
        return "TwoSideSortedPartition{" +
                "left=" + left +
                ", right=" + right +
                '}';
    }
}
