package leveretconey.util;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface  leveretconeyTest{
    int times() default 1;
    boolean outputTime() default true;
    boolean enabled() default true;
    CreateInstanceTiming createInstanceTiming() default CreateInstanceTiming.CALL;


    public enum CreateInstanceTiming{
        CLASS,METHOD,CALL
    }
}
