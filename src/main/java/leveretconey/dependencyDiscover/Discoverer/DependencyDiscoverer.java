package leveretconey.dependencyDiscover.Discoverer;

import java.util.Collection;

import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Dependency.AbstractDependency;

public interface DependencyDiscoverer<E extends AbstractDependency> {
    public Collection<E> discover(DataFrame data);
}
