package leveretconey.dependencyDiscover.Validator;

import leveretconey.cocoa.twoSideExpand.ALODTree;
import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Dependency.LexicographicalOrderDependency;
import leveretconey.dependencyDiscover.Validator.Result.ApproximateDependencyValidationResult;
import leveretconey.dependencyDiscover.Validator.Result.ValidationResultWithBound;

public interface ALODValidator extends ApproximateDependencyValidator<LexicographicalOrderDependency> {
    default ValidationResultWithBound validate(DataFrame data, ALODTree.ALODTreeNode node){
        return validate(data,node.toLOD());
    }

    @Override
    ValidationResultWithBound validate(DataFrame data, LexicographicalOrderDependency dependency);
}
