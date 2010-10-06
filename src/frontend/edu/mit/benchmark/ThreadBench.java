package edu.mit.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadBench {
    private BenchmarkState testState;
    private final List<? extends Worker> workers;

    private static enum State {
        WARMUP,
        MEASURE,
        DONE,
        EXIT,
    }

    private static final class BenchmarkState {
        private final int queueLimit;
        private volatile State state = State.WARMUP;

        private final CountDownLatch startBarrier;
        private AtomicInteger notDoneCount;

        // Protected by this
        private int workAvailable = 0;
        private int workersWaiting = 0;

        /**
         * 
         * @param numThreads number of threads involved in the test: including the master thread.
         * @param rateLimited
         * @param queueLimit
         */
        public BenchmarkState(int numThreads, boolean rateLimited,
                int queueLimit) {
            this.queueLimit = queueLimit;
            startBarrier = new CountDownLatch(numThreads);
            notDoneCount = new AtomicInteger(numThreads);

            assert numThreads > 0;
            if (!rateLimited) {
                workAvailable = -1;
            } else {
                assert queueLimit > 0;
            }
        }

        public State getState() {
            return state;
        }

        /**
         * Wait for all threads to call this. Returns once all the threads have
         * entered.
         */
        public void blockForStart() {
            assert state == State.WARMUP;
            assert startBarrier.getCount() > 0;
            startBarrier.countDown();
            try {
                startBarrier.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        public void startMeasure() {
            assert state == State.WARMUP;
            state = State.MEASURE;
        }

        public void startCoolDown() {
            assert state == State.MEASURE;
            state = State.DONE;

            // The master thread must also signal that it is done
            signalDone();
        }

        /** Notify that this thread has entered the done state. */
        private void signalDone() {
            assert state == State.DONE;
            int current = notDoneCount.decrementAndGet();
            assert current >= 0;

            if (current == 0) {
                // We are the last thread to notice that we are done: wake any blocked workers
                state = State.EXIT;
                synchronized (this) {
                    if (workersWaiting > 0) {
                        this.notifyAll();
                    }
                }
            }
        }

        public boolean isRateLimited() {
            // Should be thread-safe due to only being used during
            // initialization
            return workAvailable != -1;
        }

        /** Add a request to do work. */
        public void addWork(int amount) {
            assert amount > 0;

            synchronized (this) {
                assert workAvailable >= 0;

                workAvailable += amount;

                if (workAvailable > queueLimit) {
                    throw new RuntimeException("Work queue limit ("
                            + queueLimit
                            + ") exceeded; Cannot keep up with desired rate");
                }

                if (workersWaiting <= amount) {
                    // Wake all waiters
                    this.notifyAll();
                } else {
                    // Only wake the correct number of waiters
                    assert workersWaiting > amount;
                    for (int i = 0; i < amount; ++i) {
                        this.notify();
                    }
                }
                int wakeCount = (workersWaiting < amount) ? workersWaiting
                        : amount;
                assert wakeCount <= workersWaiting;
            }
        }

        /** Called by ThreadPoolThreads when waiting for work. */
        public State fetchWork() {
            synchronized (this) {
                if (workAvailable == 0) {
                    workersWaiting += 1;
                    while (workAvailable == 0) {
                        if (state == State.EXIT) {
                            return State.EXIT;
                        }
                        try {
                            this.wait();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    workersWaiting -= 1;
                }

                assert workAvailable > 0;
                workAvailable -= 1;

                return state;
            }
        }

    }

    public static abstract class Worker implements Runnable {
        private BenchmarkState testState;
        private int measuredRequests = 0;

        @Override
        public final void run() {
            // In case of reuse reset the measured requests
            measuredRequests = 0;
            boolean isRateLimited = testState.isRateLimited();

            // wait for start
            testState.blockForStart();

//            System.out.println(this + " start");
            boolean seenDone = false;
            State state = testState.getState();
            while (state != State.EXIT) {
                if (state == State.DONE && !seenDone) {
                    // This is the first time we have observed that the test is done
                    // notify the global test state, then continue applying load
                    seenDone = true;
                    testState.signalDone();
                }

                // apply load
                if (isRateLimited) {
                    // re-reads the state because it could have changed if we blocked
                    state = testState.fetchWork();
                }

//                long start;
//                if (isRateLimited && state == State.MEASURE) {
//                    start = System.nanoTime();
//                }

                boolean measure = state == State.MEASURE; 
                doWork(measure);
                if (measure) {
                    measuredRequests += 1;
//                    if (isRateLimited) {
//                        long end = System.nanoTime();
//                        addLatency(end - start);
//                    }
                }
                state = testState.getState();
            }

            tearDown();
            testState = null;
        }

        public int getRequests() {
            return measuredRequests;
        }

        /** Called in a loop in the thread to exercise the system under test. */
        protected abstract void doWork(boolean measure);

        /** Called at the end of the test to do any clean up that may be required. */
        protected void tearDown() {}

        public void setBenchmark(BenchmarkState testState) {
            assert this.testState == null;
            this.testState = testState;
        }
    }

    private ThreadBench(List<? extends Worker> workers) {
        this.workers = workers;
    }

    public static final class Results {
        public final long nanoSeconds;
        public final int measuredRequests;

        public Results(long nanoSeconds, int measuredRequests) {
            this.nanoSeconds = nanoSeconds;
            this.measuredRequests = measuredRequests;
        }

        public double getRequestsPerSecond() {
            return (double) measuredRequests / (double) nanoSeconds * 1e9;
        }

        @Override
        public String toString() {
            return "Results(nanoSeconds=" + nanoSeconds + ", measuredRequests=" +
                    measuredRequests + ") = " + getRequestsPerSecond() + " requests/sec"; 
        }
    }

    public Results runRateLimited(int warmUpSeconds, int measureSeconds, int requestsPerSecond) {
        assert requestsPerSecond > 0;

        ArrayList<Thread> workerThreads = createWorkerThreads(true);

        final long intervalNs = (long) (1000000000. / (double) requestsPerSecond + 0.5); 

        testState.blockForStart();

        long start = System.nanoTime();
        long measureStart = start + warmUpSeconds * 1000000000L;
        long measureEnd = -1;
 
        long nextInterval = start + intervalNs;
        int nextToAdd = 1;
        while (true) {
            testState.addWork(nextToAdd);

            // Wait until the interval expires, which may be "don't wait"
            long now = System.nanoTime();
            long diff = nextInterval - now;
            while (diff > 0) {  // this can wake early: sleep mulitple times to avoid that
                long ms = diff / 1000000;
                diff = diff % 1000000;
                try {
                    Thread.sleep(ms, (int) diff);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                now = System.nanoTime();
                diff = nextInterval - now;
            }
            assert diff <= 0;

            // Compute how many messages to deliver
            nextToAdd = (int)(-diff / intervalNs + 1);
            assert nextToAdd > 0;
            nextInterval += intervalNs * nextToAdd;

            // Update the test state appropriately
            State state = testState.getState();
            if (state == State.WARMUP && now >= measureStart) {
                testState.startMeasure();
                measureStart = now;
                measureEnd = measureStart + measureSeconds * 1000000000L;
            } else if (state == State.MEASURE && now >= measureEnd) {
                testState.startCoolDown();
                measureEnd = now;
            } else if (state == State.EXIT) {
                // All threads have noticed the done, meaning all measured requests have definitely finished.
                // Time to quit.
                break;
            }
        }

        try {
            int requests = waitForThreadExit(workerThreads);
            return new Results(measureEnd - measureStart, requests);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private ArrayList<Thread> createWorkerThreads(boolean isRateLimited) {
        assert testState == null;
        testState = new BenchmarkState(workers.size() + 1, isRateLimited, RATE_QUEUE_LIMIT);

        ArrayList<Thread> workerThreads = new ArrayList<Thread>(workers.size());
        for (Worker worker : workers) {
            worker.setBenchmark(testState);
            Thread thread = new Thread(worker);
            thread.start();
            workerThreads.add(thread);
        }
        return workerThreads;
    }

    private static final int RATE_QUEUE_LIMIT = 1000;

    public Results run(int warmUpSeconds, int measureSeconds) {
        ArrayList<Thread> workerThreads = createWorkerThreads(false);

        try {
            testState.blockForStart();
            Thread.sleep(warmUpSeconds * 1000);
            long startNanos = System.nanoTime();
            testState.startMeasure();
            Thread.sleep(measureSeconds * 1000);
            testState.startCoolDown();
            long endNanos = System.nanoTime();

            int requests = waitForThreadExit(workerThreads);

            long ns = endNanos - startNanos;
            return new Results(ns, requests);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private int waitForThreadExit(ArrayList<Thread> workerThreads)
            throws InterruptedException {
        assert testState.getState() == State.DONE || testState.getState() == State.EXIT;
        int requests = 0;
        for (int i = 0; i < workerThreads.size(); ++i) {
            workerThreads.get(i).join();
            requests += workers.get(i).getRequests();
        }
        testState = null;
        return requests;
    }

    public static Results runBenchmark(List<? extends Worker> workers,
            int warmUpSeconds, int measureSeconds) {
        ThreadBench bench = new ThreadBench(workers);
        return bench.run(warmUpSeconds, measureSeconds);
    }

    public static Results runRateLimitedBenchmark(List<? extends Worker> workers,
            int warmUpSeconds, int measureSeconds, int requestsPerSecond) {
        ThreadBench bench = new ThreadBench(workers);
        return bench.runRateLimited(warmUpSeconds, measureSeconds, requestsPerSecond);
    }
}
