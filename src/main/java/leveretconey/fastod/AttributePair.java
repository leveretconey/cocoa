package leveretconey.fastod;

import java.security.InvalidParameterException;
import java.util.Objects;

import leveretconey.dependencyDiscover.Predicate.SingleAttributePredicate;

public class AttributePair {
    public final SingleAttributePredicate left;
    public final int right;

    public AttributePair(SingleAttributePredicate left, int right) {
        if(left.attribute==right){
            throw new InvalidParameterException("two attributes cannot be the same");
        }
        this.left = left;
        this.right = right;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AttributePair)) return false;
        AttributePair that = (AttributePair) o;
        return right == that.right && left==that.left;
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right);
    }

    @Override
    public String toString() {
        return String.format("{%s,%d}", left, right +1);
    }


}
