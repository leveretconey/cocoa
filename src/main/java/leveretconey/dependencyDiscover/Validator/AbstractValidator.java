package leveretconey.dependencyDiscover.Validator;

public interface AbstractValidator {
    default boolean isResultConfirmed(){
        return true;
    }
}
