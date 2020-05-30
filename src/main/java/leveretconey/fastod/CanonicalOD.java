package leveretconey.fastod;

import java.util.Objects;

import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Predicate.Operator;
import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicate;

public class CanonicalOD implements Comparable<CanonicalOD>{

    public AttributeSet context;
    public int right;
    public SingleAttributePredicate left;
    public static int splitCheckCount=0;
    public static int swapCheckCount=0;

    @Override
    public int compareTo(CanonicalOD o) {
        int attributeCountDifference=context.getAttributeCount()-o.context.getAttributeCount();
        if(attributeCountDifference!=0)
            return attributeCountDifference;
        int contextValueDiff=context.getValue()-o.context.getValue();
        if(contextValueDiff!=0)
            return contextValueDiff;
        int rightDiff=right-o.right;
        if (rightDiff!=0)
            return rightDiff;
        if (left!=null) {
            int leftDiff = left.attribute - o.left.attribute;
            if (leftDiff != 0)
                return leftDiff;
            if (left.operator == o.left.operator)
                return 0;
            if (left.operator == Operator.lessEqual)
                return -1;
        }
        return 0;

    }

    public CanonicalOD(AttributeSet context, SingleAttributePredicate left, int right) {
        this.context = context;
        this.right = right;
        this.left = left;
    }

    public CanonicalOD(AttributeSet context, int right) {
        this.context = context;
        this.right = right;
        this.left=null;
    }

    @Override
    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append(context).append(" : ");
        if(left==null){
            sb.append("[] -> ");
        }else {
            sb.append(left).append(" ~ ");
        }
        sb.append(right+1).append("<=");
        return sb.toString();
    }

    public boolean isValid(DataFrame data, double errorRateThreshold){
        StrippedPartition sp= StrippedPartition.getStrippedPartition(context,data);
        if (errorRateThreshold==-1f){
            if(left==null){
                splitCheckCount++;
                return !sp.split(right);
            }
            else {
                swapCheckCount++;
                return !sp.swap(left, right);
            }
        }else {
            long vioCount;
            if (left == null) {
                vioCount = sp.splitRemoveCount(right);
            } else {
                vioCount = sp.swapRemoveCount(left,right);
            }
            double errorRate = (double) vioCount /data.getTupleCount();
            return errorRate<errorRateThreshold;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CanonicalOD)) return false;
        CanonicalOD that = (CanonicalOD) o;
        return right == that.right &&
                left == that.left &&
                context.equals(that.context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(context, right, left);
    }

}
