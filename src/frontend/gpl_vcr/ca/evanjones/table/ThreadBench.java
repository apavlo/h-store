package ca.evanjones.table;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThreadBench {
    public static abstract class Worker extends Thread {
        private ThreadBench parent;

        public final void run() {
            // wait for start
            parent.startBarrier.countDown();
            try {
                parent.startBarrier.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

//            System.out.println(this + " start");
            boolean measured = false;
            boolean setBarrier = false;
            while (!parent.end.get()) {
                boolean measure = parent.measure.get();
                if (measure && !measured) {
//                    System.out.println(this + " measure");
                    measured = true;
                } else if (!measure && measured) {
                    // we have exited the measurement period
//                    System.out.println(this + " measure done");
                    parent.endBarrier.countDown();
                    measured = false;
                    setBarrier = true;
                }
                doWork(measure);
            }
            if (!setBarrier) parent.endBarrier.countDown();
//            System.out.println(this + " exit");
        }

        private void setParent(ThreadBench parent) {
            this.parent = parent;
        }

        /** Called in a loop in the thread to exercise the system under test. */
        protected abstract void doWork(boolean measure);
    }

    private final AtomicBoolean end = new AtomicBoolean(false);
    private final AtomicBoolean measure = new AtomicBoolean();
    private final CountDownLatch startBarrier;
    private final CountDownLatch endBarrier;
    private final List<? extends Worker> workers;

    public ThreadBench(List<? extends Worker> workers) {
        startBarrier = new CountDownLatch(workers.size() + 1);
        endBarrier = new CountDownLatch(workers.size());
        this.workers = workers;
    }

    public long run(int warmUpSeconds, int measureSeconds) {
        for (Worker worker : workers) {
            worker.setParent(this);
            worker.start();
        }

        try {
            startBarrier.countDown();
            startBarrier.await();
            Thread.sleep(warmUpSeconds * 1000);
            long startNanos = System.nanoTime();
            measure.set(true);
            while (measureSeconds-- > 0 && !end.get()) {
                Thread.sleep(1000);
            }
            measure.set(false);
            long endNanos = System.nanoTime();
            endBarrier.await();
            end.set(true);

            for (Worker worker : workers) {
                worker.join();
            }

            return endNanos - startNanos;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    
    public void stop() {
        end.set(true);
    }

    /** @return the measured duration of the measurement period, in nanoseconds. */
    public static long runBenchmark(List<? extends Worker> workers, int warmUpSeconds, int measureSeconds) {
        ThreadBench bench = new ThreadBench(workers);
        return bench.run(warmUpSeconds, measureSeconds);
    }
}
