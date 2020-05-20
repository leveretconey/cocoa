package leveretconey.cocoa.sample;

import leveretconey.util.Util;

public class MathUtil {
    public static final double EPS = 1e-8;
    private MathUtil(){

    }
    public static double[] getConfidenceIntervalOfBinomialDistribution(double p, int n){
        double std=Math.sqrt( p * (1-p) / n );
        return new double[]{p-3*std,p+3*std};
    }

    public static interface Equation{
        double f(double x);
    }

    public static double solveEquation(double low , double high, Equation equation){
        double lowValue=equation.f(low),highValue=equation.f(high);
        while (high-low > EPS){
            if (lowValue>0 == highValue > 0){
                throw new RuntimeException("Equation can not be solved");
            }

            double mid=(low+high)/2;
            double midValue=equation.f(mid);
            if (lowValue>0 != midValue>0){
                high=mid;
                highValue=midValue;
            }else {
                low=mid;
                lowValue=midValue;
            }
        }
        return low;
    }

    public static void main(String[] args) {
        double p=solveEquation(0,0.01,(x)->x+3*Math.sqrt(x * (1-x) / 10000) -0.01);
        Util.out(p);
    }
}
