package leveretconey.fastod;

import java.security.InvalidParameterException;

public class AttributePair {
    public final int attribute1;
    public final int attribute2;

    public AttributePair(int attribute1, int attribute2) {
        if(attribute1==attribute2){
            throw new InvalidParameterException("two attributes cannot be the same");
        }
        this.attribute1 = Math.min(attribute1,attribute2);
        this.attribute2 = Math.max(attribute1,attribute2);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AttributePair that = (AttributePair) o;
        return attribute1 == that.attribute1 &&
                attribute2 == that.attribute2;
    }

    @Override
    public String toString() {
        return String.format("{%d,%d}",attribute1+1,attribute2+1);
    }

    @Override
    public int hashCode() {
        return 1<<attribute1 | 1<<attribute2;
    }
}
