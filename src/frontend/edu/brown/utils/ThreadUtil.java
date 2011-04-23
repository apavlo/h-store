package edu.brown.utils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;
import org.voltdb.utils.Pair;

public abstract class ThreadUtil {
    private static final Logger LOG = Logger.getLogger(ThreadUtil.class);

    private static final Object lock = new Object();
    private static ExecutorService pool;
    
    private static final int DEFAULT_NUM_THREADS = 4;
    
    
    /**
     * Convenience wrapper around Thread.sleep() for when we don't care about exceptions
     * @param millis
     */
    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            // IGNORE!
        }
    }

    /**
     * Executes the given command and returns a pair containing the PID and Process handle
     * @param command
     * @return
     */
    public static Pair<Integer, Process> exec(String command[]) {
        ProcessBuilder pb = new ProcessBuilder(command);
        Process p = null;
        try {
            p = pb.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert(p != null);
        Class<? extends Process> p_class = p.getClass();
        assert(p_class.getName().endsWith("UNIXProcess")) : "Unexpected Process class: " + p_class;
        
        Integer pid = null;
        try {
            Field pid_field = p_class.getDeclaredField("pid");
            pid_field.setAccessible(true);
            pid = pid_field.getInt(p);
        } catch (Exception ex) {
            LOG.fatal("Faild to get pid for " + p, ex);
            return (null);
        }
        assert(pid != null) : "Failed to get pid for " + p;
        
        LOG.info("Starting new process with PID " + pid);
        return (Pair.of(pid, p));
    }
    
    /**
     * Fork the command (in the current thread)
     * @param command
     */
    public static void fork(String command[], EventObservable stop_observable) {
        ThreadUtil.fork(command, stop_observable, null, false);
    }
    
    /**
     * 
     * @param command
     * @param prefix
     * @param stop_observable
     * @param print_output
     */
    public static void fork(String command[], final EventObservable stop_observable, final String prefix, final boolean print_output) {
        final boolean debug = LOG.isDebugEnabled(); 
        
        if (debug) LOG.debug("Forking off process: " + Arrays.toString(command));

        // Copied from ShellTools
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        Process temp = null;
        try {
            temp = pb.start();
        } catch (IOException e) {
            LOG.fatal("Failed to fork command", e);
            return;
        }
        assert(temp != null);
        final Process p = temp;
        
        // Register a observer if we have a stop observable
        if (stop_observable != null) {
            final String prog_name = FileUtil.basename(command[0]);
            stop_observable.addObserver(new EventObserver() {
                boolean first = true;
                @Override
                public void update(Observable arg0, Object arg1) {
                    assert(first) : "Trying to stop the process twice??";
                    if (debug) LOG.debug("Stopping Process -> " + prog_name);
                    p.destroy();
                    first = false;
                }
            });
        }

        if (print_output) {
            BufferedInputStream in = new BufferedInputStream(p.getInputStream());
            StringBuilder buffer = new StringBuilder();
            int c;
            try {
                while((c = in.read()) != -1) {
                    buffer.append((char)c);
                    if (((char)c) == '\n') {
                        System.out.print(prefix + buffer.toString());
                        buffer = new StringBuilder();
                    }
                }
            } catch (Exception e) {
                p.destroy();
            }
            if (buffer.length() > 0) System.out.println(prefix + buffer);
        }
        
        try {
            p.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        p.destroy();
    }
    
    /**
     * Get the max number of threads that will be allowed to run concurrenctly in the global pool
     * @return
     */
    public static int getMaxGlobalThreads() {
        int max_threads = DEFAULT_NUM_THREADS;
        String prop = System.getProperty("hstore.max_threads");
        if (prop != null && prop.startsWith("${") == false) max_threads = Integer.parseInt(prop);
        return (max_threads);
    }
    
    /**
     * 
     * @param <R>
     * @param threads
     */
    public static <R extends Runnable> void runGlobalPool(final Collection<R> threads) {
        final boolean d = LOG.isDebugEnabled();
        
        // Initialize the thread pool the first time that we run
        synchronized (ThreadUtil.lock) {
            if (ThreadUtil.pool == null) {
                int max_threads = ThreadUtil.getMaxGlobalThreads();
                if (d) LOG.debug("Creating new fixed thread pool [num_threads=" + max_threads + "]");
                ThreadUtil.pool = Executors.newFixedThreadPool(max_threads, factory);
            }
        } // SYNCHRONIZED
        
        ThreadUtil.run(threads, ThreadUtil.pool, false);
    }
    
    /**
     * 
     * @param <R>
     * @param threads
     */
    public static <R extends Runnable> void runNewPool(final Collection<R> threads) {
        ExecutorService pool = Executors.newCachedThreadPool(factory);
        ThreadUtil.run(threads, pool, true);
    }
    
    /**
     * 
     * @param <R>
     * @param threads
     */
    public static <R extends Runnable> void runNewPool(final Collection<R> threads, int max_concurrent) {
        ExecutorService pool = Executors.newFixedThreadPool(max_concurrent, factory);
        ThreadUtil.run(threads, pool, true);
    }
    
    /**
     * For a given list of threads, execute them all (up to max_concurrent at a time) and return
     * once they have completed. If max_concurrent is null, then all threads will be fired off at the same time
     * @param threads
     * @param max_concurrent
     * @throws Exception
     */
    private static final <R extends Runnable> void run(final Collection<R> threads, final ExecutorService pool, final boolean stop_pool) {
        final boolean d = LOG.isDebugEnabled();
        final long start = System.currentTimeMillis();
        
        int num_threads = threads.size();
        CountDownLatch latch = new CountDownLatch(num_threads);
        
        if (d) LOG.debug(String.format("Executing %d threads and blocking until they finish", num_threads));
        for (R r : threads) {
            pool.execute(new LatchRunnable(r, latch));
        } // FOR
        if (stop_pool) pool.shutdown();
        
        try {
            latch.await();
        } catch (InterruptedException ex) {
            LOG.fatal("ThreadUtil.run() was interuptted!", ex);
            throw new RuntimeException(ex);
        }
        if (d) {
            final long stop = System.currentTimeMillis();
            LOG.debug(String.format("Finished executing %d threads [time=%.02fs]", num_threads, (stop-start)/1000d));
        }
        return;
    }
    
    private static final ThreadFactory factory = new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return (t);
        }
    };
    
    private static class LatchRunnable implements Runnable {
        private final Runnable r;
        private final CountDownLatch latch;
        
        public LatchRunnable(Runnable r, CountDownLatch latch) {
            this.r = r;
            this.latch = latch;
        }
        @Override
        public void run() {
            this.r.run();
            this.latch.countDown();
        }
    }
    
}
