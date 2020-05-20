package leveretconey.cocoa.twoSideExpand;

import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Dependency.LexicographicalOrderDependency;
import leveretconey.dependencyDiscover.SortedPartition.SortedPartition;
import leveretconey.dependencyDiscover.SortedPartition.TwoSideSortedPartition;
import leveretconey.dependencyDiscover.Validator.ALODValidator;
import leveretconey.dependencyDiscover.Validator.Result.ValidationResultWithBound;

public class BruteForceALODValidatorUsingSP implements ALODValidator {
    @Override
    public ValidationResultWithBound validate(DataFrame data, LexicographicalOrderDependency dependency) {
        return new TwoSideSortedPartition(data,dependency).validateForALODWithG1();
    }
}
