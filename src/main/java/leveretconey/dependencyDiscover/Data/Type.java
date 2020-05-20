package leveretconey.dependencyDiscover.Data;

import java.util.Comparator;
import java.util.regex.Pattern;

public interface Type{
    boolean fitFormat(String s);
    Object parse(String s);
    Comparator getComparator();
}
