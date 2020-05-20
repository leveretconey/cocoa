package leveretconey.dependencyDiscover.Data;

import java.util.Comparator;

public class TypeNull extends AbstractType{
    public TypeNull() {
        super("");
    }

    @Override
    public Object parse(String s) {
        return "";
    }

    @Override
    public boolean fitFormat(String s) {
        return "".equals(s);
    }

    @Override
    public Comparator getComparator() {
        return (n1,n2)->0;
    }
}
