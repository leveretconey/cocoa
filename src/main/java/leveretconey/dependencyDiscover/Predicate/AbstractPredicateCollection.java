package leveretconey.dependencyDiscover.Predicate;

public interface AbstractPredicateCollection<E extends AbstractPredicate> extends Iterable<E>{

    public boolean add(E predicate);
    public void remove(E predicate);
    default public boolean contains(E predicate){
        return contains(predicate,false);
    }
    public boolean contains(E predicate, boolean isSerious);
    public int size();
}
