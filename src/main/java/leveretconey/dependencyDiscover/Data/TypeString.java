package leveretconey.dependencyDiscover.Data;

import java.util.Comparator;

public class TypeString extends AbstractType{

    public TypeString() {
        super(".*");
    }

    @Override
    public boolean fitFormat(String s) {
        return true;
    }

    @Override
    public String parse(String s) {
        return s;
    }

    @Override
    public Comparator<String> getComparator() {return String::compareTo;}
}
