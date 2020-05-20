package leveretconey.dependencyDiscover.Data;

import java.util.Comparator;

public class TypeLong extends AbstractType{

    public TypeLong() {
        super("[\\+-]?([1-9]\\d*|0)");
    }

    @Override
    public Long parse(String s) {
        if ("".equals(s))
            return Long.MIN_VALUE;
        try {
            return Long.parseLong(s);
        }catch (Exception e){
            if(s.charAt(0)=='-')
                return Long.MIN_VALUE;
            else
                return Long.MAX_VALUE;
        }

    }

    @Override
    public Comparator<Long> getComparator() {
        return Long::compareTo;
    }
}
