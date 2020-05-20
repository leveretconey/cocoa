package leveretconey.dependencyDiscover.Validator.Result;

import java.util.ArrayList;
import java.util.List;

public class LODValidationResult implements DependencyValidationResult {
    public LODStatus status= LODStatus.UNKNOWN;
    public List<Integer> violationRows;

    public LODValidationResult(LODStatus status) {
        this();
        this.status = status;
    }

    public LODValidationResult() {
        violationRows=new ArrayList<>();
    }


    @Override
    public boolean isValid() {
        return status==LODStatus.VALID;
    }

    @Override
    public String toString() {
        return "LODValidationResult{" +
                "status=" + status +
                ", violationRows=" + violationRows +
                '}';
    }

    public enum LODStatus{
        VALID,SWAP,SPLIT,UNKNOWN;

        @Override
        public String toString() {
            if(equals(VALID))
                return "V";
            else if(equals(SWAP))
                return "W";
            else if(equals(SPLIT))
                return "L";
            else if(equals(UNKNOWN))
                return "N";
            return "?";
        }
    }
}
