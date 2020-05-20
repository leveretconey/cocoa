package leveretconey.dependencyDiscover.Validator.Result;

public interface ApproximateDependencyValidationResult extends  ValidationResult{
    public boolean isValid(double errorRateThreshold);
    default public boolean isConfirmed(){
        return true;
    }
}
