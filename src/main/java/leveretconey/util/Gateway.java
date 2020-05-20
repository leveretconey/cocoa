package leveretconey.util;

import java.util.Random;

public interface Gateway {
    boolean isOpen();

    public static class CountGateway implements Gateway{
        private int openEveryTestCount;
        private long testCount=0;

        public CountGateway(int openEveryTestCount) {
            this.openEveryTestCount = openEveryTestCount;
        }

        @Override
        public boolean isOpen() {
            testCount++;
            return testCount % openEveryTestCount==0;
        }
    }

    public static class ProbabilityGateway implements Gateway{
        private int averageOpenEveryTestCount;
        private Random random=new Random();

        public ProbabilityGateway(int averageOpenEveryTestCount) {
            this.averageOpenEveryTestCount = averageOpenEveryTestCount;
        }

        @Override
        public boolean isOpen() {
            return random.nextInt(averageOpenEveryTestCount)==0;
        }
    }

    public static class FixedTimeGateway implements Gateway{
        private long interval;
        private Timer timer;

        public FixedTimeGateway(long interval) {
            this.interval = interval;
            timer=new Timer();
        }

        @Override
        public boolean isOpen() {
            if (timer.getTimeUsed() >= interval){
                timer.reset();
                return true;
            }else {
                return false;
            }
        }
    }

    public static Gateway AlwaysClose=()->false;
    public static Gateway AlwaysOpen=()->true;

    public static class LogGateway {
        public static final int TRACE=0;
        public static final int DEBUG=1;
        public static final int INFO=2;
        public static final int WARN=3;
        public static final int ERROR=4;

        private static int level=DEBUG;

        private LogGateway() {
        }

        public static void setLevel(int level) {
            LogGateway.level = level;
        }

        public static boolean isOpen(int level){
            return level>= LogGateway.level;
        }
    }

    public static class ComplexTimeGateway implements Gateway{
        private Timer timer=new Timer();
        private int accCount=0;
        private long interval=1000;

        @Override
        public boolean isOpen() {
            if (timer.getTimeUsed()>interval){
                timer.reset();
                accCount++;
                if (accCount==10){
                    accCount=0;
                    interval*=2;
                }
                return true;
            }else {
                return false;
            }
        }
    }
}
