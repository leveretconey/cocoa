package leveretconey.cocoa.sample;

import leveretconey.cocoa.twoSideExpand.ALODTree.ALODTreeNode;
import leveretconey.dependencyDiscover.Data.DataFrame;
import leveretconey.dependencyDiscover.Dependency.LexicographicalOrderDependency;
import leveretconey.dependencyDiscover.Validator.ALODValidator;
import leveretconey.dependencyDiscover.Validator.Result.ApproximateDependencyValidationResult;
import leveretconey.dependencyDiscover.Validator.Result.ValidationResultWithAccurateBound;

public class ValidatorForG1UsingISPCache implements ALODValidator {

    private DFSISPCache ispCacheUsingDependency;
    private DFSISPCacheAttachedToNode ispCacheUsingNode;

    public ValidatorForG1UsingISPCache(DFSISPCache ispCacheUsingDependency, DFSISPCacheAttachedToNode ispCacheUsingNode) {
        this.ispCacheUsingDependency = ispCacheUsingDependency;
        this.ispCacheUsingNode = ispCacheUsingNode;
    }

    public ValidatorForG1UsingISPCache(DFSISPCacheAttachedToNode ispCacheUsingNode) {
        this.ispCacheUsingNode = ispCacheUsingNode;
    }

    public ValidatorForG1UsingISPCache(DFSISPCache ispCacheUsingDependency) {
        this.ispCacheUsingDependency = ispCacheUsingDependency;
    }

    @Override
    public ValidationResultWithAccurateBound validate(DataFrame data, LexicographicalOrderDependency dependency) {
        return ispCacheUsingDependency.getISP(dependency).validateForALODWithG1();
    }

    @Override
    public ValidationResultWithAccurateBound validate(DataFrame data, ALODTreeNode node) {
        return ispCacheUsingNode.getISP(node).validateForALODWithG1();
    }
}
