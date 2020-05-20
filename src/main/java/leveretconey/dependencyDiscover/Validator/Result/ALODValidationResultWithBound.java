package leveretconey.dependencyDiscover.Validator.Result;

import leveretconey.dependencyDiscover.Validator.Result.ValidationResultWithBound;

public class ALODValidationResultWithBound extends ValidationResultWithAccurateBound {


    public ALODValidationResultWithBound(double errorRate, double errorRateLowerBound, double errorRateUpperBound) {
        super(errorRate, errorRateLowerBound, errorRateUpperBound);
    }

    public ALODValidationResultWithBound(double errorRate, boolean isConfirmed, double errorRateLowerBound, double errorRateUpperBound) {
        super(errorRate, isConfirmed, errorRateLowerBound, errorRateUpperBound);
    }

    public ALODValidationResultWithBound(long splitViolationCount, long swapViolationCount,
                                         long neverViolationCount, long totalTuplePairCount, boolean isErrorRateConfirmed){
        super(
         (double) (swapViolationCount+splitViolationCount)/totalTuplePairCount,
                isErrorRateConfirmed,
                (double)swapViolationCount/totalTuplePairCount,
                1-(double)neverViolationCount/totalTuplePairCount
        );

    }
}
