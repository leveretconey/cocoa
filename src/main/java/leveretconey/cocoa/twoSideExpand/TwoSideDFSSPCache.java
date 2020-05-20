package leveretconey.cocoa.twoSideExpand;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import leveretconey.dependencyDiscover.Predicate.Operator;
import leveretconey.dependencyDiscover.SortedPartition.SortedPartition;
import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicate;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicateList;
import leveretconey.dependencyDiscover.SPCache.SortedPartitionCache;
import leveretconey.util.Statistics;
import leveretconey.util.Util;

public class TwoSideDFSSPCache implements SortedPartitionCache {
    private Map<SingleAttributePredicateList,SortedPartition> noRemoveCache;
    private Map<SingleAttributePredicateList,SortedPartition> mayRemoveCache;

    private int maxCacheSize;

    public  TwoSideDFSSPCache(DataFrame data) {
        this(data,1000);
    }

    public TwoSideDFSSPCache(DataFrame data, int maxCacheSize) {
        this.maxCacheSize = maxCacheSize;

        noRemoveCache=new HashMap<>();
        mayRemoveCache=new LinkedHashMap<SingleAttributePredicateList, SortedPartition>(){
            @Override
            protected boolean removeEldestEntry(Map.Entry<SingleAttributePredicateList, SortedPartition> eldest) {
                return noRemoveCache.size() + mayRemoveCache.size()>=maxCacheSize;
            }
        };

        noRemoveCache.put(new SingleAttributePredicateList(),new SortedPartition(data));
        for (int attribute = 0; attribute < data.getColumnCount(); attribute++) {
            SingleAttributePredicate predicate=SingleAttributePredicate
                    .getInstance(attribute, Operator.lessEqual);
            SortedPartition sp=new SortedPartition(data);
            sp.intersect(data,predicate);
            noRemoveCache.put(new SingleAttributePredicateList(predicate),sp);

            predicate=SingleAttributePredicate
                    .getInstance(attribute, Operator.greaterEqual);
            sp=new SortedPartition(data);
            sp.intersect(data,predicate);
            noRemoveCache.put(new SingleAttributePredicateList(predicate),sp);
        }
    }

    @Override
    public SortedPartition get(SingleAttributePredicateList list) {
        SortedPartition sp=null;
        if (noRemoveCache.containsKey(list)){
            Statistics.addCount("sp缓存命中");
            return noRemoveCache.get(list);
        }
        if (mayRemoveCache.containsKey(list)){
            Statistics.addCount("sp缓存命中");
            sp= mayRemoveCache.get(list);
            mayRemoveCache.remove(list);
        }else {
            Statistics.addCount("sp缓存未命中");
        }
        if (sp==null) {
            SingleAttributePredicateList oneLess = list.deepCloneAndRemoveLast();
            SingleAttributePredicate expandPredicate = list.get(oneLess.size());
            sp = noRemoveCache.get(oneLess).deepClone();
            sp.intersect(get(expandPredicate));
        }
        noRemoveCache.put(list,sp);
        return sp;
    }

    private SortedPartition get(SingleAttributePredicate predicate){
        return noRemoveCache.get(new SingleAttributePredicateList(predicate));
    }

    public void mayRemove(SingleAttributePredicateList list){
        if (list.size()<=1){
            return;
        }
        SortedPartition sp= noRemoveCache.getOrDefault(list,null);
        if (sp!=null){
            noRemoveCache.remove(list);
            mayRemoveCache.put(list,sp);
        }
    }
//
//    private void save(SingleAttributePredicateList list,SortedPartition sp){
////        if (noRemoveCache.size()+ mayRemoveCache.size()>=maxCacheSize && !mayRemoveCache.isEmpty()){
////            int countKeysToRemove= Math.min(noRemoveCache.size() + mayRemoveCache.size()-maxCacheSize,mayRemoveCache.size());
////            List<SingleAttributePredicateList> keysToRemove=new ArrayList<>();
////            for (SingleAttributePredicateList key : mayRemoveCache.keySet()) {
////                keysToRemove.add(key);
////                if(keysToRemove.size()==countKeysToRemove ){
////                    break;
////                }
////            }
////            Statistics.addCount("sp缓存删除");
////            for (SingleAttributePredicateList key : keysToRemove) {
////                mayRemoveCache.remove(key);
////            }
////        }
////        (list, sp);
//    }
}
