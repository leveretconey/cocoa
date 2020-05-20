package leveretconey.fastod;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.util.Timer;

public class StrippedPartition {
    public List<Integer> indexes;
    public List<Integer> begins;
    private DataFrame data;
    public static long mergeTime=0;
    public static long validateTime=0;
    public static long cloneTime=0;
    public static final int CACHE_SIZE=10000;
    private static Map<AttributeSet, StrippedPartition> cache=new LinkedHashMap<AttributeSet, StrippedPartition>(){
        @Override
        protected boolean removeEldestEntry(Map.Entry<AttributeSet, StrippedPartition> eldest) {
            return size()>=CACHE_SIZE;
        }
    };

    public StrippedPartition(DataFrame data) {
        this.data = data;

        indexes=new ArrayList<>();
        for (int i = 0; i < data.getTupleCount(); i++) {
            indexes.add(i);
        }
        begins=new ArrayList<>();
        if(data.getTupleCount()!=0){
            begins.add(0);
        }
        begins.add(data.getTupleCount());

    }
    public StrippedPartition(StrippedPartition origin){
        this.indexes=new ArrayList<>(origin.indexes);
        this.begins=new ArrayList<>(origin.begins);
        this.data=origin.data;
    }



    public StrippedPartition product(int attribute){
        Timer timer=new Timer();
        List<Integer> newIndexes=new ArrayList<>();
        List<Integer> newBegins=new ArrayList<>();
        int fillPointer=0;
        for(int beginPointer=0;beginPointer<begins.size()-1;beginPointer++){
            int groupBegin=begins.get(beginPointer);
            int groupEnd=begins.get(beginPointer+1);
            HashMap<Integer,List<Integer>> subGroups=new HashMap<>();

            for(int i=groupBegin;i<groupEnd;i++){
                int index= indexes.get(i);
                int value=data.get(index,attribute);
                if(!subGroups.containsKey(value)){
                    subGroups.put(value,new ArrayList<>());
                }
                subGroups.get(value).add(index);
            }


            for(List<Integer> newGroup:subGroups.values()){
                if(newGroup.size()>1){
                    newBegins.add(fillPointer);
                    for (int i :newGroup) {
                        newIndexes.add(i);
                        fillPointer++;
                    }
                }
            }
        }

        this.indexes=newIndexes;
        this.begins=newBegins;
        begins.add(indexes.size());
        mergeTime+=timer.getTimeUsed();
        return this;
    }

    public boolean split(int right){
        Timer timer=new Timer();
        for(int beginPointer=0;beginPointer<begins.size()-1;beginPointer++) {
            int groupBegin = begins.get(beginPointer);
            int groupEnd = begins.get(beginPointer + 1);

            int groupValue=data.get(indexes.get(groupBegin),right);
            for(int i=groupBegin+1;i<groupEnd;i++) {
                int index = indexes.get(i);
                int value = data.get(index, right);
                if(value!=groupValue) {
                    validateTime+=timer.getTimeUsed();
                    return true;
                }
            }
        }
        validateTime+=timer.getTimeUsed();
        return false;
    }

    public boolean swap(int left, int right){
        Timer timer=new Timer();
        for(int beginPointer=0;beginPointer<begins.size()-1;beginPointer++) {
            int groupBegin = begins.get(beginPointer);
            int groupEnd = begins.get(beginPointer + 1);

            List<DataAndIndex> values=new ArrayList<>();
            for(int i=groupBegin;i<groupEnd;i++) {
                int index = indexes.get(i);
                values.add(new DataAndIndex(data.get(index,left),data.get(index,right)));
            }
            Collections.sort(values);
            int beforeMax=Integer.MIN_VALUE;
            int groupMax=Integer.MIN_VALUE;
            for (int i = 0; i < values.size(); i++) {
                int index=values.get(i).index;
                if(i==0 || values.get(i-1).data != values.get(i).data){
                    beforeMax=Math.max(groupMax,beforeMax);
                    groupMax=index;
                }else {
                    groupMax=Math.max(groupMax,index);
                }
                if(index<beforeMax) {
                    validateTime+=timer.getTimeUsed();
                    return true;
                }
            }
        }
        validateTime+=timer.getTimeUsed();
        return false;
    }

    @Override
    public String toString() {
        return "StrippedPartition{" +
                "indexes=" + indexes +
                ", begins=" + begins +
                '}';
    }

    public StrippedPartition deepClone(){
        Timer timer=new Timer();
        StrippedPartition result=new StrippedPartition(this.data);
        result.indexes= new ArrayList<>(indexes);
        result.begins= new ArrayList<>(begins);
        cloneTime+=timer.getTimeUsed();
        return result;
    }

    public static StrippedPartition getStrippedPartition(AttributeSet attributeSet, DataFrame data){
        if(cache.containsKey(attributeSet)){
            return cache.get(attributeSet);
        }
        StrippedPartition result=null;
        for(int attribute:attributeSet){
            AttributeSet oneLess=attributeSet.deleteAttribute(attribute);
            if(cache.containsKey(oneLess)){
                result=cache.get(oneLess).deepClone().product(attribute);
            }
        }
        if(result==null){
            result=new StrippedPartition(data);
            for (int attribute : attributeSet) {
                result.product(attribute);
            }
        }
        cache.put(attributeSet,result);
        return result;
    }

    public long splitRemoveCount(int right){
        Timer timer=new Timer();
        long result=0;
        for(int beginPointer = 0; beginPointer<begins.size()-1; beginPointer++) {

            int groupBegin = begins.get(beginPointer);
            int groupEnd = begins.get(beginPointer + 1);
            int groupLength=groupEnd-groupBegin;
            HashMap<Integer,Integer> groupInt2count=new HashMap<>();

            for (int i = groupBegin ; i < groupEnd; i++) {
                int rightValue=data.get(indexes.get(i),right);
                groupInt2count.put(rightValue,groupInt2count.getOrDefault(rightValue,0)+1);
            }
            int max=Integer.MIN_VALUE;
            for (int count : groupInt2count.values()) {
                max=Math.max(max,count);
            }
            result+=groupLength-max;
        }
        validateTime+=timer.getTimeUsed();
        return result;
    }

    public long swapRemoveCount(int left, int right){
        int length=indexes.size();
        int[] vioCount=new int[length];
        boolean[] deleted=new boolean[length];
        int result=0;
        nextClass:
        for(int beginPointer = 0; beginPointer<begins.size()-1; beginPointer++) {
            int groupBegin = begins.get(beginPointer);
            int groupEnd = begins.get(beginPointer + 1);
            for (int i = groupBegin ; i < groupEnd; i++) {
                int lefti=data.get(indexes.get(i),left);
                int righti=data.get(indexes.get(i),right);
                for (int j = i+1 ; j < groupEnd; j++) {
                    int diffLeft=lefti-data.get(indexes.get(j),left);
                    int diffRight=righti-data.get(indexes.get(j),right);
                    if (diffLeft!=0 && diffRight!=0 && (diffLeft>0 != diffRight>0)){
                        vioCount[i]++;
                        vioCount[j]++;
                    }
                }
            }
            while (true){
                int deleteIndex=-1;
                for (int i = groupBegin ; i < groupEnd; i++) {
                    if (!deleted[i] &&(deleteIndex==-1 || vioCount[i]>vioCount[deleteIndex])){
                        deleteIndex=i;
                    }
                }
                if (deleteIndex==-1 || vioCount[deleteIndex]==0){
                    continue nextClass;
                }
                result++;
                deleted[deleteIndex]=true;
                int leftj=data.get(indexes.get(deleteIndex),left);
                int rightj=data.get(indexes.get(deleteIndex),right);
                for (int i = groupBegin ; i < groupEnd; i++) {
                    int diffLeft=leftj-data.get(indexes.get(i),left);
                    int diffRight=rightj-data.get(indexes.get(i),right);
                    if (diffLeft!=0 && diffRight!=0 && (diffLeft>0 != diffRight>0)){
                        vioCount[i]--;
                    }
                }
            }
        }
        return result;
    }
}

