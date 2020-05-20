package leveretconey.fastod;

import java.util.Objects;

import leveretconey.dependencyDiscover.Data.DataFrame;

public class CanonicalOD implements Comparable<CanonicalOD>{

    public AttributeSet context;
    public int right;
    public int left;
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
        int leftDiff=left-o.left;
        if(leftDiff!=0)
            return leftDiff;
        return right-o.right;
    }

    public CanonicalOD(AttributeSet context, int left, int right) {
        this.context = context;
        this.right = right;
        this.left = left;
    }

    public CanonicalOD(AttributeSet context, int right) {
        this.context = context;
        this.right = right;
        this.left=-1;
    }

    @Override
    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append(context).append(" : ");
        if(left==-1){
            sb.append("[] -> ");
        }else {
            sb.append(left+1).append(" ~ ");
        }
        sb.append(right+1);
        return sb.toString();
    }

    public boolean isValid(DataFrame data, double errorRateThreshold){
        StrippedPartition sp=StrippedPartition.getStrippedPartition(context,data);
        if (errorRateThreshold==-1f){
            if(left==-1){
                splitCheckCount++;
                return !sp.split(right);
            }
            else {
                swapCheckCount++;
                return !sp.swap(left, right);
            }
        }else {
            long vioCount;
            if (left == -1) {
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

    public static CanonicalOD makeCanonicalODBeginFromOne(AttributeSet context, int right){
        return makeCanonicalODBeginFromOne(context,0, right);
    }
    public static CanonicalOD makeCanonicalODBeginFromOne(AttributeSet context, int left, int right){
        AttributeSet set=new AttributeSet();
        for(int attribute:context){
            set=set.addAttribute(attribute-1);
        }
        return new CanonicalOD(set,left-1,right-1);
    }
}
