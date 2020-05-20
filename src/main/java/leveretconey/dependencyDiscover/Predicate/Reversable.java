package leveretconey.dependencyDiscover.Predicate;

public interface Reversable<E extends AbstractPredicateCollection> {
    public boolean isReverse(E l);
}
