package leveretconey.cocoa.ranking;

import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Dependency.LexicographicalOrderDependency;
import leveretconey.dependencyDiscover.SortedPartition.ImprovedTwoSideSortedPartition;
import leveretconey.dependencyDiscover.SortedPartition.SortedPartition;

public class LODRFClassSquareAttriCountAver implements LODRankingFunction {
    private DataFrame data;
    private double alpha;

    public LODRFClassSquareAttriCountAver(DataFrame data, double alpha) {
        this.data = data;
        this.alpha = alpha;
    }

    @Override
    public double getRanking(LexicographicalOrderDependency od, ImprovedTwoSideSortedPartition isp) {
        return alpha/2*(getInterestingness(isp.left)+getInterestingness(isp.right))
                +(1-alpha)*getEasyness(od);
    }

    private double getInterestingness(SortedPartition sp){
        long s=sp.getTupleCount() * sp.getTupleCount();
        for(int i=0;i<sp.begins.size()-1;i++){
            int classSize=sp.begins.get(i+1)-sp.begins.get(i);
            s-=classSize*classSize;
        }
        return (double)s/(sp.getTupleCount()*sp.getTupleCount());
    }

    private double getEasyness(LexicographicalOrderDependency od){
        return 1-(double)od.length()/data.getColumnCount();
    }
}
