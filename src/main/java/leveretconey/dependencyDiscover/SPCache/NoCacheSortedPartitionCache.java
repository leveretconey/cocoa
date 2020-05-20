package leveretconey.dependencyDiscover.SPCache;

import leveretconey.dependencyDiscover.SortedPartition.SortedPartition;
import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicate;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicateList;

public class NoCacheSortedPartitionCache implements SortedPartitionCache{

    private DataFrame data;

    public NoCacheSortedPartitionCache (DataFrame data){

        this.data = data;
    }
    @Override
    public SortedPartition get(SingleAttributePredicateList list) {
        SortedPartition sp=new SortedPartition(data);
        for (SingleAttributePredicate predicate : list) {
            sp.intersect(data,predicate);
        }
        return sp;
    }
}
