package leveretconey.cocoa.multipleStandard;

import leveretconey.dependencyDiscover.Dependency.LexicographicalOrderDependency;
import leveretconey.dependencyDiscover.SortedPartition.ImprovedTwoSideSortedPartition;

public interface LODRankingFunction {
    public double getRanking(LexicographicalOrderDependency od,
                             ImprovedTwoSideSortedPartition sp);
}
