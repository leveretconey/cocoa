package leveretconey.util;

import java.util.HashMap;
import java.util.Map;

public class Statistics {

    private static Map<String,Integer> map=new HashMap<>();

    private Statistics(){

    }

    public static void addCount(String name,int count){
        map.put(name,map.getOrDefault(name,0)+count);
    }

    public static void addCount(String name){
        addCount(name,1);
    }

    public static void reset(){
        map.clear();
    }
    public static void printStstistics(){
        StringBuilder sb=new StringBuilder();
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            sb.append(entry.getKey()).append(':').append(entry.getValue()).append(",");
        }
        if (sb.length()>0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        Util.out(sb.toString());
    }
}
