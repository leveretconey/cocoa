package leveretconey.cocoa.ranking;

import leveretconey.dependencyDiscover.Dependency.LexicographicalOrderDependency;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicateList;
import leveretconey.dependencyDiscover.SortedPartition.ImprovedTwoSideSortedPartition;
import leveretconey.dependencyDiscover.SortedPartition.SortedPartition;

public class LODRFClassSquareSumOverAttriCount implements LODRankingFunction {
    @Override
    public double getRanking(LexicographicalOrderDependency od, ImprovedTwoSideSortedPartition sp) {
        return (getInterestingness(sp.left,od.left)+ getInterestingness(sp.right,od.right))/2;
    }

    private double getInterestingness(SortedPartition sp, SingleAttributePredicateList list){
        long s=sp.getTupleCount() * sp.getTupleCount();
        for(int i=0;i<sp.begins.size()-1;i++){
            int classSize=sp.begins.get(i+1)-sp.begins.get(i);
            s-=classSize*classSize;
        }
        return (double)s/(sp.getTupleCount()*sp.getTupleCount())/list.size();
    }
}
