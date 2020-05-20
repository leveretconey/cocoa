package leveretconey.util;

import java.lang.reflect.Method;

public class Test {

    public static <T> void testTime(Class[] cs) {
        testTime(cs,null);
    }
    public static <T> void testTime(Class[] cs, Object[] args){
        for (Class c : cs) {
            testTime(c,args);
        }
    }
    public static <T> void testTime(Class<T> c){
        testTime(c,null);
    }
    public static <T> void testTime(Class<T> c, Object[] args){
        try {
            Object o=c.newInstance();


            for (Method method : c.getDeclaredMethods()) {
                for (leveretconeyTest annotation : method.getAnnotationsByType(leveretconeyTest.class)) {
                    if(annotation.times()<=0 || !annotation.enabled())
                        continue;
                    method.setAccessible(true);
                    if(annotation.createInstanceTiming()== leveretconeyTest.CreateInstanceTiming.METHOD)
                        o=c.newInstance();
                    StringBuilder sb=new StringBuilder();
                    sb.append("run ").append(c.getCanonicalName())
                            .append(method.getName()).append(" ")
                            .append(annotation.times()).append(" times");
                    int times=annotation.times();
                    Timer timer=new Timer();
                    for (int i = 0; i < times; i++) {
                        if(annotation.createInstanceTiming()== leveretconeyTest.CreateInstanceTiming.CALL)
                            o=c.newInstance();
                        if(args==null)
                            method.invoke(o);
                        else
                            method.invoke(o,args);
                    }
                    if(annotation.outputTime()) {
                        sb.append(", using ").append(timer.getTimeUsed()/1000.0).append(" s");
                    }
                    Util.out(sb);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
