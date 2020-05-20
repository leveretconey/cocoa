package leveretconey.util;

@Deprecated
public class TimedJob<E> {

    private static final int SLEEP_TIME=1;

    public E start(long expireTime, Job<E> job) {
        Timer timer =new Timer();
        Runner<E> runner=new Runner<>(job);
        Thread thread=new Thread(runner::run);
        thread.start();
        while (timer.getTimeUsed()<expireTime){
            try {
                Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (thread.getState()== Thread.State.TERMINATED){
                return runner.result;
            }
        }
        thread.stop();
        return null;
    }

    public interface Job<E>{
        public E run() ;
    }
    private class Runner<E>{
        E result;
        Job<E> job;

        public Runner(Job<E> job) {
            this.job = job;
        }
        void run(){
            result=job.run();
        }
    }
}
