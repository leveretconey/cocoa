package leveretconey.dependencyDiscover.Data;

import java.util.Comparator;

public class TypeDouble extends AbstractType{

    public TypeDouble() {
        super("[\\+-]?(([1-9]\\d*|0)(\\.\\d+)?|(\\.\\d+))([eE][\\+-]?(([1-9]\\d*)|0))?");

    }

    @Override
    public Double parse(String s) {
        if(s.equals(""))
            return Double.NEGATIVE_INFINITY;
        return Double.parseDouble(s);
    }

    @Override
    public Comparator<Double> getComparator() {
        return Double::compareTo;
    }
}
