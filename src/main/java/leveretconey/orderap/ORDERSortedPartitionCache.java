package leveretconey.orderap;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Predicate.Operator;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicate;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicateList;
import leveretconey.dependencyDiscover.SPCache.SortedPartitionCache;
import leveretconey.dependencyDiscover.SortedPartition.SortedPartition;

public class ORDERSortedPartitionCache implements SortedPartitionCache {
    private Map<SingleAttributePredicateList,SortedPartition> onePredicateCache;
    private Map<SingleAttributePredicateList,SortedPartition> lruCache;

    private DataFrame data;
    private int maxCacheSize;

    public ORDERSortedPartitionCache(DataFrame data) {
        this(data,1000);
    }

    public ORDERSortedPartitionCache(DataFrame data, int maxCacheSize) {
        this.data = data;
        this.maxCacheSize = maxCacheSize;

        onePredicateCache =new HashMap<>();
        lruCache =new LinkedHashMap<SingleAttributePredicateList, SortedPartition>(){
            @Override
            protected boolean removeEldestEntry(Map.Entry<SingleAttributePredicateList, SortedPartition> eldest) {
                return onePredicateCache.size() + lruCache.size()>=maxCacheSize;
            }
        };

        onePredicateCache.put(new SingleAttributePredicateList(),new SortedPartition(data));
        for (int attribute = 0; attribute < data.getColumnCount(); attribute++) {
            SingleAttributePredicate predicate=SingleAttributePredicate
                    .getInstance(attribute, Operator.lessEqual);
            SortedPartition sp=new SortedPartition(data);
            sp.intersect(data,predicate);
            onePredicateCache.put(new SingleAttributePredicateList(predicate),sp);

            predicate=SingleAttributePredicate
                    .getInstance(attribute, Operator.greaterEqual);
            sp=new SortedPartition(data);
            sp.intersect(data,predicate);
            onePredicateCache.put(new SingleAttributePredicateList(predicate),sp);
        }
    }

    @Override
    public SortedPartition get(SingleAttributePredicateList list) {
        if (list.size()==1){
            return onePredicateCache.get(list);
        }
        if (lruCache.containsKey(list)){
            return lruCache.get(list);
        }
        SortedPartition sp=null;
        SingleAttributePredicateList oneLess = list.deepCloneAndRemoveLast();
        if (lruCache.containsKey(oneLess)){
            SingleAttributePredicate expandPredicate = list.get(oneLess.size());
            sp = onePredicateCache.get(oneLess).deepClone();
            sp.intersect(get(expandPredicate));
        }else {
            sp=new SortedPartition(data);
            for (SingleAttributePredicate predicate : list) {
                sp.intersect(get(predicate));
            }
        }
        lruCache.put(list,sp);
        return sp;
    }

    private SortedPartition get(SingleAttributePredicate predicate){
        return onePredicateCache.get(new SingleAttributePredicateList(predicate));
    }

}
