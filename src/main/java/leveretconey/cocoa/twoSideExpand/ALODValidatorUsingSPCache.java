package leveretconey.cocoa.twoSideExpand;

import leveretconey.dependencyDiscover.SortedPartition.TwoSideSortedPartition;
import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Dependency.LexicographicalOrderDependency;
import leveretconey.dependencyDiscover.SPCache.SortedPartitionCache;
import leveretconey.dependencyDiscover.Validator.ALODValidator;
import leveretconey.dependencyDiscover.Validator.Result.ValidationResultWithBound;

public class ALODValidatorUsingSPCache implements ALODValidator {

    private SortedPartitionCache spCache;

    public ALODValidatorUsingSPCache(SortedPartitionCache spCache){

        this.spCache = spCache;
    }

    @Override
    public ValidationResultWithBound validate(DataFrame data
            , LexicographicalOrderDependency dependency) {
        return new TwoSideSortedPartition(spCache.get(dependency.left),spCache.get(dependency.right))
                .validateForALODWithG1();
    }

}
