package leveretconey.dependencyDiscover.Validator.Result;

public class ValidationResultWithBoundResultOnly extends ValidationResultWithResultOnly implements ValidationResultWithBound{
    public boolean lowerBoundGreater=false;
    public boolean upperBoundLess=false;

    public ValidationResultWithBoundResultOnly(boolean isValid, boolean isConfirmed, boolean lowerBoundGreater, boolean upperBoundLess) {
        super(isValid, isConfirmed);
        this.lowerBoundGreater = lowerBoundGreater;
        this.upperBoundLess = upperBoundLess;
    }

    public ValidationResultWithBoundResultOnly(boolean isValid, boolean lowerBoundGreater, boolean upperBoundLess) {
        super(isValid);
        this.lowerBoundGreater = lowerBoundGreater;
        this.upperBoundLess = upperBoundLess;
    }

    @Override
    public boolean lowerBoundGreaterThan(double errorRateThreshold) {
        return lowerBoundGreater;
    }

    @Override
    public boolean upperBoundLessThan(double errorRateThreshold) {
        return upperBoundLess;
    }

    @Override
    public String toString() {
        return String.format("(%s)%s (%b %b)",isConfirmed?"confirmed":"estimated",
                isValid?"valid":"invalid",lowerBoundGreater,lowerBoundGreater);
    }
}
