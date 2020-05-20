package leveretconey.dependencyDiscover.Validator;

import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Dependency.AbstractDependency;
import leveretconey.dependencyDiscover.Validator.Result.ApproximateDependencyValidationResult;

public interface ApproximateDependencyValidator<E extends AbstractDependency> extends AbstractValidator {

    ApproximateDependencyValidationResult validate(DataFrame data, E dependency);
}
