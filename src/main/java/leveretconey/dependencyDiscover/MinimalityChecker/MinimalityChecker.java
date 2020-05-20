package leveretconey.dependencyDiscover.MinimalityChecker;

import leveretconey.dependencyDiscover.Dependency.AbstractDependency;

interface MinimalityChecker<E extends AbstractDependency> {
    void insert(E dependency);
    boolean isMinimal(E dependency);
}
