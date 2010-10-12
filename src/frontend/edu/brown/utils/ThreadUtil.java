package edu.brown.utils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

public abstract class ThreadUtil {
    private static final Logger LOG = Logger.getLogger(ThreadUtil.class);

    /**
     * Fork the command (in the current thread) and countdown the latch everytime we see
     * our match string in the output
     * @param command
     * @param match
     * @param latch
     */
    public static void forkLatch(String command[], String match, final CountDownLatch latch, final EventObservable stop_observable) {
        LOG.debug("Forking off process: " + Arrays.toString(command));

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
            stop_observable.addObserver(new EventObserver() {
                boolean first = true;
                @Override
                public void update(Observable arg0, Object arg1) {
                    assert(first) : "Trying to stop the process twice??";
                    LOG.info("Stopping Process " + p);
                    p.destroy();
                    first = false;
                }
            });
        }
        
        BufferedInputStream in = new BufferedInputStream(p.getInputStream());
        StringBuilder buffer = new StringBuilder();
        int c;
        try {
            while((c = in.read()) != -1) {
                buffer.append((char)c);
                
                // As soon as we see our match message, let the next guy go!
                // We reset the buffer so that we can correctly identify the next match
                if (latch.getCount() > 0 && buffer.toString().contains(match)) {
                    latch.countDown();
                    buffer = new StringBuilder();
                }
            }
        } catch (Exception e) {
            p.destroy();
        }
        try {
            p.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        p.destroy();
    }
    
    /**
     * For a given list of threads, execute them all at the same time and return once they have completed
     * @param threads
     * @throws Exception
     */
    public static void run(final List<? extends Thread> orig_threads) throws Exception {
        ThreadUtil.run(orig_threads, -1);
    }
    
    /**
     * For a given list of threads, execute them all (up to max_concurrent at a time) and return
     * once they have completed
     * @param threads
     * @param max_concurrent
     * @throws Exception
     */
    public static void run(final List<? extends Thread> orig_threads, int max_concurrent) throws Exception {
        // Make a new list of threads so that we can modify its contents without affecting
        // the data structures of whoever called us.
        List<Thread> threads = new ArrayList<Thread>(orig_threads);
        List<Thread> running = new Vector<Thread>();
        
        LOG.debug("Executing " + threads.size() + " threads [max_concurrent=" + max_concurrent + "]");
        long max_sleep = 16000;
        while (!threads.isEmpty() || !running.isEmpty()) {
            while ((max_concurrent < 0 || running.size() < max_concurrent) && !threads.isEmpty()) {
                Thread thread = threads.remove(0);
                thread.start();
                running.add(thread);
                LOG.debug("Started " + thread);
                LOG.debug("Running=" + running.size() + ", Waiting=" + threads.size() + ", Available=" + (max_concurrent - running.size()));
            } // WHILE
            int num_running = running.size();
            long sleep = 1000;
            while (num_running > 0) {
                for (int i = 0; i < num_running; i++) {
                    Thread thread = running.get(i);
                    thread.join(sleep);
                    if (!thread.isAlive()) {
                        running.remove(i);
                        LOG.debug(thread + " is complete");
                        LOG.debug("Running=" + running.size() + ", Waiting=" + threads.size() + ", Available=" + (max_concurrent - running.size()));
                        break;
                    }
                } // FOR
                if (num_running != running.size()) break;
                sleep *= 2;
                if (sleep > max_sleep) sleep = max_sleep;
            } // WHILE
        } // WHILE
        LOG.debug("All threads are finished");
        return;
    }
    
}
