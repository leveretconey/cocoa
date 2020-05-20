package leveretconey.dependencyDiscover.Data;

import java.util.regex.Pattern;

public abstract class AbstractType implements Type{
    private Pattern pattern;

    public AbstractType(String regex) {
        this.pattern = Pattern.compile(regex);
    }

    @Override
    public boolean fitFormat(String s) {
        return pattern.matcher(s).matches();
    }
}
