package leveretconey.cocoa.twoSideExpand;

import java.util.Random;

import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Dependency.LexicographicalOrderDependency;
import leveretconey.dependencyDiscover.Validator.ALODValidator;
import leveretconey.dependencyDiscover.Validator.Result.ALODValidationResultWithBound;
import leveretconey.dependencyDiscover.Validator.Result.ValidationResultWithBound;
import leveretconey.util.TimeStatistics;

public class RandomSampleEstimatingALODValidator implements ALODValidator {
    private int sampleCount;
    private Random random;

    public RandomSampleEstimatingALODValidator(int sampleCount, Random random) {
        this.sampleCount = sampleCount;
        this.random = random;
    }

    public RandomSampleEstimatingALODValidator(int sampleCount) {
        this.sampleCount = sampleCount;
        random=new Random();
    }

    public RandomSampleEstimatingALODValidator() {
        this(10000);
    }

    @Override
    public ValidationResultWithBound validate(DataFrame data, LexicographicalOrderDependency dependency) {
        TimeStatistics.TimeStopper timer = TimeStatistics.start("g1近似计算");
        int splitCount=0,swapCount=0,neverViolationCount=0;
        int tupleCount=data.getTupleCount();
        for (int i = 0; i < sampleCount; i++) {
            int tuple1=random.nextInt(tupleCount);
            int tuple2=random.nextInt(tupleCount-1);
            if (tuple2>=tuple1){
                tuple2++;
            }
            int diff1=dependency.left.compare(data,tuple1,tuple2);
            if (diff1>0){
                continue;
            }
            int diff2=dependency.right.compare(data,tuple1,tuple2);
            if (diff1==0){
                if (diff2>0){
                    splitCount++;
                }
            }else {
                if (diff2<0){
                    neverViolationCount++;
                }else if(diff2>0){
                    swapCount++;
                }
            }
        }
        timer.stop();
        return new ALODValidationResultWithBound(splitCount,swapCount,neverViolationCount,sampleCount,false);
    }
}

