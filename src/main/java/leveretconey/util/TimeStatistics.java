package leveretconey.util;

import java.util.HashMap;
import java.util.Map;

public class TimeStatistics {

    private static Map<String,Long> times=new HashMap<>();

    public static TimeStopper start(String name){
        return new TimeStopper(name);
    }

    public static long getTime(String name){
        return times.get(name);
    }

    public static void reset(){
        times.clear();
    }
    public static void printStatistics(){
        long sum=0;
        StringBuilder sb=new StringBuilder();
        for (Map.Entry<String, Long> entry : times.entrySet()) {
            if (sb.length()!=0){
                sb.append(',');
            }
            sum+=entry.getValue();
            sb.append(entry.getKey()).append("用时").append(String.format("%.3f秒",entry.getValue()/1000.0));
        }
        sb.append(String.format(",总计%.3f秒",sum/1000.0));
        Util.out(sb);
    }

    private TimeStatistics() {
    }

    public static class TimeStopper{
        private String name;
        private Timer timer;

        private TimeStopper(String name) {
            this.name = name;
            timer=new Timer();
        }

        public void stop(){
            times.put(name,times.getOrDefault(name,0L)+timer.getTimeUsed());
        }
    }
}
