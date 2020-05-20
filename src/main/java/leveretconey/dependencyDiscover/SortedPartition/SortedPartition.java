package leveretconey.dependencyDiscover.SortedPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javafx.util.Pair;
import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Predicate.Operator;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicate;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicateList;
import leveretconey.util.Statistics;
import leveretconey.util.TimeStatistics;
import leveretconey.util.Util;

public class SortedPartition {
    public int[] indexes;
    public List<Integer> begins;
    public int[] index2groupIndex;



    private final static Comparator<Pair<Integer,Integer>> greaterComparator =(i1, i2)->i2.getKey()-i1.getKey();
    private final static Comparator<Pair<Integer,Integer>> lessComparator =(i1, i2)->i1.getKey()-i2.getKey();



    private SortedPartition(){
    }
    public SortedPartition(DataFrame data, SingleAttributePredicateList list) {
        this(data);
        for (SingleAttributePredicate predicate : list) {
            intersect(data,predicate);
        }
    }
    public SortedPartition(DataFrame data) {
        int tupleCount=data.getTupleCount();
        indexes=new int[tupleCount];
        for (int i = 0; i < tupleCount; i++) {
            indexes[i]=i;
        }
        begins=new ArrayList<>();
        begins.add(0);
        begins.add(tupleCount);
        index2groupIndex=new int[tupleCount];
        updateIndex2IndexGroup();
    }

    public SortedPartition intersect(DataFrame data, SingleAttributePredicate predicate){

        Statistics.addCount("用谓词扩展sp次数");
        if(isUnique())
            return this;
        TimeStatistics.TimeStopper timer = TimeStatistics.start("sp扩展");
        int column=predicate.attribute;
        Comparator<Pair<Integer,Integer>> comparator=
                (predicate.operator== Operator.lessEqual)
                ? lessComparator : greaterComparator;

        List<Integer> newBegins=new ArrayList<>();
        for(int beginPointer=0;beginPointer<begins.size()-1;beginPointer++) {
            int groupBegin = begins.get(beginPointer);
            int groupEnd = begins.get(beginPointer + 1);
            if(groupBegin==groupEnd-1){
                newBegins.add(groupBegin);
                continue;
            }
            int value = data.get(indexes[groupBegin], column);
            boolean same=true;
            List<Pair<Integer,Integer>> mergeData=new ArrayList<>();
            for(int i=groupBegin;i<groupEnd;i++){
                int row=indexes[i];
                int rowValue=data.get(row,column);
                if(rowValue!=value){
                    same=false;
                }
                mergeData.add(new Pair<>(rowValue,row));
            }
            if(same){
                newBegins.add(groupBegin);
                continue;
            }
            mergeData.sort(comparator);
            int fillPointer=groupBegin;
            for (int i = 0; i < mergeData.size(); i++) {
                if(i==0 || !mergeData.get(i-1).getKey().equals(mergeData.get(i).getKey())){
                    newBegins.add(fillPointer);
                }
                indexes[fillPointer]=mergeData.get(i).getValue();
                fillPointer++;
            }
        }
        begins=newBegins;
        begins.add(indexes.length);

        updateIndex2IndexGroup();
        timer.stop();
        return this;
    }

    public SortedPartition intersect(SortedPartition another){

        Statistics.addCount("用sp扩展sp次数");
        if(isUnique()) {
            return this;
        }
        TimeStatistics.TimeStopper timer = TimeStatistics.start("sp扩展");

        int tupleCount=getTupleCount();
        int originalGroupCount=getGroupCount();
        int[] newBeginsInArray=new int[tupleCount];
        int[] fillPointers=new int[originalGroupCount];
        for(int i=0;i<originalGroupCount;i++){
            fillPointers[i]=begins.get(i);
        }
        int[] groupLastRightIndex=new int[originalGroupCount];
        Arrays.fill(groupLastRightIndex,-1);
        int[] groupVisitedCount=new int[originalGroupCount];

        for(int indexInRight=0;indexInRight<another.begins.size()-1;indexInRight++) {
            int groupBegin = another.begins.get(indexInRight);
            int groupEnd = another.begins.get(indexInRight + 1);
            for(int i=groupBegin;i<groupEnd;i++) {
                int row = another.indexes[i];
                int groupIndexInLeft=index2groupIndex[row];
                if (groupLastRightIndex[groupIndexInLeft] <  indexInRight){
                    groupLastRightIndex[groupIndexInLeft] =  indexInRight;
                    int fillPointer=fillPointers[groupIndexInLeft];
                    fillPointers[groupIndexInLeft]++;
                    newBeginsInArray[fillPointer]=begins.get(groupIndexInLeft)
                            +groupVisitedCount[groupIndexInLeft];
                }
                indexes[begins.get(groupIndexInLeft) +groupVisitedCount[groupIndexInLeft]]=row;
                groupVisitedCount[groupIndexInLeft]++;
            }

        }
        begins.clear();
        begins.add(0);
        for (int x : newBeginsInArray) {
            if (x!=0){
                begins.add(x);
            }
        }
        begins.add(indexes.length);

        updateIndex2IndexGroup();
        timer.stop();
        return this;
    }

    @Deprecated
    public SortedPartition intersectOld(SortedPartition another){

        Statistics.addCount("用sp扩展sp次数");
        if(isUnique()) {
            return this;
        }
        TimeStatistics.TimeStopper timer = TimeStatistics.start("sp扩展");
        List<Integer> newBegins=new ArrayList<>();
        for(int beginPointer=0;beginPointer<begins.size()-1;beginPointer++) {
            int groupBegin = begins.get(beginPointer);
            int groupEnd = begins.get(beginPointer + 1);
            if(groupBegin==groupEnd-1){
                newBegins.add(groupBegin);
                continue;
            }
            int value = another.index2groupIndex[indexes[groupBegin]];
            boolean same=true;
            List<Pair<Integer,Integer>> mergeData=new ArrayList<>();
            for(int i=groupBegin;i<groupEnd;i++){
                int row=indexes[i];
                int rowValue=another.index2groupIndex[indexes[i]];
                if(rowValue!=value){
                    same=false;
                }
                mergeData.add(new Pair<>(rowValue,row));
            }
            if(same){
                newBegins.add(groupBegin);
                continue;
            }
            mergeData.sort(lessComparator);
            int fillPointer=groupBegin;
            for (int i = 0; i < mergeData.size(); i++) {
                if(i==0 || !mergeData.get(i-1).getKey().equals(mergeData.get(i).getKey())){
                    newBegins.add(fillPointer);
                }
                indexes[fillPointer]=mergeData.get(i).getValue();
                fillPointer++;
            }
        }
        begins=newBegins;
        begins.add(indexes.length);

        updateIndex2IndexGroup();
        timer.stop();
        return this;
    }

    private void updateIndex2IndexGroup(){
        int rightGroupIndex=-1;
        int beginPointer=0;
        for (int i = 0; i < indexes.length; i++) {
            if(i== begins.get(beginPointer)){
                rightGroupIndex++;
                beginPointer++;
            }
            index2groupIndex[indexes[i]]=rightGroupIndex;
        }
    }

    public SortedPartition deepClone(){
        TimeStatistics.TimeStopper timer = TimeStatistics.start("sp拷贝");
        SortedPartition result=new SortedPartition();
        result.indexes = Arrays.copyOf(indexes, indexes.length);
        result.begins = new ArrayList<>(begins);
        result.index2groupIndex=Arrays.copyOf(index2groupIndex,index2groupIndex.length);
        timer.stop();
        return result;
    }

    public boolean isUnique(){
        return indexes.length==begins.size()-1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SortedPartition)) return false;
        SortedPartition that = (SortedPartition) o;
        return Arrays.equals(indexes,that.indexes)
                && begins.equals(that.begins);
    }



    public boolean equalsFast(SortedPartition anotherSp){
        return begins.size()==anotherSp.begins.size() && indexes.length==anotherSp.indexes.length;

    }

    @Override
    public int hashCode() {
        int result = Objects.hash(begins);
        result = 31 * result + Arrays.hashCode(indexes);
        return result;
    }

    @Override
    public String toString() {
        return String.format("indexSize=%d,groupSize=%d",indexes.length,begins.size()-1);
    }

    //调用的时候保证每个删除的元组都存在，且其所在的等价类的长度正好是1
    void removeTuples(Set<Integer> tuplesToRemove){
        if (tuplesToRemove==null || tuplesToRemove.size()==0) {
            return;
        }
        int[] newIndexes=new int[indexes.length-tuplesToRemove.size()];
        List<Integer> newBegins=new ArrayList<>();
        newBegins.add(0);
        int indexFillPointer=0;

        for(int beginPointer=0;beginPointer<begins.size()-1;beginPointer++) {
            int groupBegin = begins.get(beginPointer);
            int tupleIndex=indexes[groupBegin];
            if (tuplesToRemove.contains(tupleIndex)){
                continue;
            }
            int groupEnd = begins.get(beginPointer + 1);
            for(int i=groupBegin;i<groupEnd;i++){
                try {
                    newIndexes[indexFillPointer++]=indexes[i];
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            newBegins.add(indexFillPointer);
        }

        indexes=newIndexes;
        begins=newBegins;
        updateIndex2IndexGroup();
    }

    public int getGroupLengthConcerningIndex(int index){
        int groupIndex=index2groupIndex[index];
        return begins.get(groupIndex+1)-begins.get(groupIndex);
    }


    public int getTupleCount(){
        return indexes.length;
    }
    public int getGroupCount(){
        return begins.size()-1;
    }
}
