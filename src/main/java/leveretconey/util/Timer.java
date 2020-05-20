package leveretconey.util;

import java.util.logging.Logger;

public class Timer {
    private long begin;
    private long pauseTime=-1;

    public void reset(){
        begin=System.currentTimeMillis();
    }
    public Timer(){
        reset();
    }

    public long getTimeUsed(){
        if (isPausing()){
            return pauseTime-begin;
        }else {
            return System.currentTimeMillis()-begin;
        }
    }

    public double getTimeUsedInSecond(){
        return getTimeUsed()/1000.0;
    }

    public double getTimeUsedInSecondAndReset(){
        double result=getTimeUsed()/1000.0;
        reset();
        return result;
    }
    public long getTimeUsedAndReset(){
        long result= getTimeUsed();
        reset();
        return result;
    }


    public void pause(){
        pauseTime=System.currentTimeMillis();
    }
    public void goOn(){
        begin+=System.currentTimeMillis()-pauseTime;
        pauseTime=-1;
    }
    public boolean isPausing(){
        return pauseTime!=-1;
    }
}
