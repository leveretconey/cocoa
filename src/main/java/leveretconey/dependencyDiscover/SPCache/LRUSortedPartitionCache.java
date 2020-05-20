package leveretconey.dependencyDiscover.SPCache;

import java.util.LinkedHashMap;
import java.util.Map;

import leveretconey.dependencyDiscover.SortedPartition.SortedPartition;
import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicate;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicateList;

public class LRUSortedPartitionCache implements SortedPartitionCache{

    public static long cacheMiss=0;
    public static long cacheHit=0;

    private static final int DEFAULT_CAPACITY=1000;

    private LinkedHashMap<SingleAttributePredicateList,SortedPartition>
     cache=new LinkedHashMap<SingleAttributePredicateList,SortedPartition>(){
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size()>=capacity;
        }
    };
    private DataFrame data;
    private int capacity;

    public LRUSortedPartitionCache(DataFrame data) {
        this(data, DEFAULT_CAPACITY);
    }

    public LRUSortedPartitionCache(DataFrame data, int capacity) {
        this.data = data;
        this.capacity = capacity;
    }

    @Override
    public SortedPartition get(SingleAttributePredicateList list) {
        if (cache.containsKey(list)){
            cacheHit++;
            return cache.get(list);
        }else {
            cacheMiss++;
        }

        SingleAttributePredicateList listClone=list.deepClone();
        while (listClone.size()>0 && !cache.containsKey(listClone)){
            listClone.removeLastElement();
        }
        SortedPartition sp= listClone.size()==0
                ? new SortedPartition(data):cache.get(listClone);
        while (listClone.size()<list.size()){
            SingleAttributePredicate predicate = list.list.get(listClone.size());
            listClone.add(predicate);
            cache.put(listClone.deepClone(),sp.deepClone());
        }
        return sp;
    }
}
