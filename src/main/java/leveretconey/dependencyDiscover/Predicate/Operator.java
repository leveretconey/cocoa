package leveretconey.dependencyDiscover.Predicate;

public enum Operator{
    equal,less,greater,lessEqual,greaterEqual, undefined,notEqual;

    public boolean violate(int op1,int op2){
        switch (this){
            case equal:return op1!=op2;
            case less:return op1>=op2;
            case greater:return op1<=op2;
            case lessEqual:return op1>op2;
            case greaterEqual:return op1<op2;
            case notEqual:return op1==op2;
        }
        throw new RuntimeException("not isValid operator");
    }

    public boolean oppose(Operator op2){
        switch (this){
            case equal:
                if(op2==less || op2==greater || op2==notEqual){
                    return true;
                }
                break;
            case less:
                if(op2==greaterEqual || op2==equal || op2==greater){
                    return true;
                }
                break;
            case greater:
                if(op2==lessEqual || op2==equal || op2==less){
                    return true;
                }
                break;
            case lessEqual:
                if(op2==greater ){
                    return true;
                }
                break;
            case greaterEqual:
                if(op2==less ){
                    return true;
                }
                break;
            case notEqual:
                if(op2==equal){
                    return true;
                }
                break;
        }
        throw new RuntimeException("not isValid operator");
    }

    public boolean imply(Operator op2){
        if(this==op2)
            return true;
        switch (this){
            case equal:
                if(op2==lessEqual || op2==greaterEqual){
                    return true;
                }
                break;
            case less:
                if(op2==lessEqual || op2==notEqual){
                    return true;
                }
                break;
            case greater:
                if(op2==lessEqual || op2==notEqual){
                    return true;
                }
                break;
        }
        return false;
    }
    public static Operator fromInt(int value){
        switch (value){
            case 0:return undefined;
            case 1:return equal;
            case 2:return less;
            case 3:return greater;
            case 4:return lessEqual;
            case 5:return greaterEqual;
            case 6:return notEqual;
        }
        throw new RuntimeException("not isValid value");
    }
    public int toInt(){
        return this.ordinal();
    }

    public Operator reverse(){
        switch (this){
            case equal:return notEqual;
            case less:return greaterEqual;
            case greater:return lessEqual;
            case lessEqual:return greater;
            case greaterEqual:return less;
            case notEqual:return equal;
        }
        throw new RuntimeException("not isValid operator");
    }

    @Override
    public String toString() {
        switch (this){
            case undefined:return "?";
            case equal:return "=";
            case less:return "<";
            case greater:return ">";
            case lessEqual:return "<=";
            case greaterEqual:return ">=";
            case notEqual:return "!=";
        }
        throw new RuntimeException("not isValid operator");
    }

    public static Operator fromString(String s){
        switch (s){
            case "=":return equal;
            case "<":return less;
            case ">":return greater;
            case "<=":return lessEqual;
            case ">=":return greaterEqual;
            case "!=":return notEqual;
        }
        throw new RuntimeException("invalid input "+s);
    }

    public boolean isLessOrGreater(){
        return this==less || this==greater || this==greaterEqual || this==lessEqual;
    }
}
