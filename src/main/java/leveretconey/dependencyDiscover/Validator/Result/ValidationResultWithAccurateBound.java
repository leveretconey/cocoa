package leveretconey.dependencyDiscover.Validator.Result;

public class ValidationResultWithAccurateBound extends ValidationResultWithAccurateErrorRate implements ValidationResultWithBound{
    public double errorRateLowerBound=0;
    public double errorRateUpperBound=1;


    public ValidationResultWithAccurateBound(double errorRate, boolean isConfirmed, double errorRateLowerBound, double errorRateUpperBound) {
        super(errorRate, isConfirmed);
        this.errorRateLowerBound = errorRateLowerBound;
        this.errorRateUpperBound = errorRateUpperBound;
    }

    public ValidationResultWithAccurateBound(double errorRate, double errorRateLowerBound, double errorRateUpperBound) {
        super(errorRate);
        this.errorRateLowerBound = errorRateLowerBound;
        this.errorRateUpperBound = errorRateUpperBound;
    }

    @Override
    public boolean lowerBoundGreaterThan(double errorRateThreshold) {
        return errorRateLowerBound >= errorRateThreshold;
    }

    @Override
    public boolean upperBoundLessThan(double errorRateThreshold) {
        return errorRateUpperBound < errorRateThreshold;
    }

    @Override
    public boolean isConfirmed() {
        return isConfirmed;
    }

    @Override
    public String toString() {
        return String.format("(%s)%.3f%% (%.3f%%,%.3f%%)",isConfirmed?"confirmed":"estimated",
                errorRate*100,errorRateLowerBound*100,errorRateUpperBound*100);
    }
}
