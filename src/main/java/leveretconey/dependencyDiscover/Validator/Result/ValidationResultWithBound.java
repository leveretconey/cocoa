package leveretconey.dependencyDiscover.Validator.Result;

public interface ValidationResultWithBound
        extends ApproximateDependencyValidationResult {

    public boolean lowerBoundGreaterThan(double errorRateThreshold);
    public boolean upperBoundLessThan(double errorRateThreshold);
}
