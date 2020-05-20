package leveretconey.dependencyDiscover.Validator.Result;

public class ValidationResultWithAccurateErrorRate implements ApproximateDependencyValidationResult{
    public double errorRate=0;
    public boolean isConfirmed=true;


    public ValidationResultWithAccurateErrorRate(double errorRate, boolean isConfirmed) {
        this.errorRate = errorRate;
        this.isConfirmed = isConfirmed;
    }

    public ValidationResultWithAccurateErrorRate(double errorRate) {
        this.errorRate = errorRate;
    }

    @Override
    public boolean isValid(double errorRateThreshold) {
        return this.errorRate<errorRateThreshold;
    }

    @Override
    public boolean isConfirmed() {
        return isConfirmed;
    }

    @Override
    public String toString() {
        return String.format("(%s)%.3f%%",isConfirmed?"confirmed":"estimated", errorRate*100);
    }
}
