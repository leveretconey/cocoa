package leveretconey.dependencyDiscover.Validator.Result;

public class ValidationResultWithResultOnly implements ApproximateDependencyValidationResult{
    public boolean isValid;
    public boolean isConfirmed=true;

    public ValidationResultWithResultOnly(boolean isValid, boolean isConfirmed) {
        this.isValid = isValid;
        this.isConfirmed = isConfirmed;
    }

    public ValidationResultWithResultOnly(boolean isValid) {
        this.isValid = isValid;
    }

    @Override
    public boolean isValid(double errorRateThreshold) {
        return isValid;
    }

    @Override
    public boolean isConfirmed() {
        return isConfirmed;
    }

    @Override
    public String toString() {
        return String.format("(%s)%s",isConfirmed?"confirmed":"estimated",
                isValid?"valid":"invalid");
    }
}
