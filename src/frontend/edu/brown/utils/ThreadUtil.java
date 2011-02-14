package edu.brown.utils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.*;

import org.apache.log4j.Logger;

public abstract class ThreadUtil {
    private static final Logger LOG = Logger.getLogger(ThreadUtil.class);

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
        
        final String prog_name = FileUtil.basename(command[0]);
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
     * For a given list of threads, execute them all at the same time and block until they have all completed
     * @param threads
     * @throws Exception
     */
    public static <R extends Runnable> void run(final Collection<R> threads) throws Exception {
        ThreadUtil.run(threads, null);
    }
    
    /**
     * For a given list of threads, execute them all (up to max_concurrent at a time) and return
     * once they have completed. If max_concurrent is null, then all threads will be fired off at the same time
     * @param threads
     * @param max_concurrent
     * @throws Exception
     */
    public static <R extends Runnable> void run(final Collection<R> threads, Integer max_concurrent) throws Exception {
        final boolean debug = LOG.isDebugEnabled();
        
        // Make a new list of threads so that we can modify its contents without affecting
        // the data structures of whoever called us.
        final List<R> available = new ArrayList<R>(threads);
        final List<Thread> running = new ArrayList<Thread>();
        if (max_concurrent == null) max_concurrent = -1;
        
        if (debug) LOG.debug("Executing " + available.size() + " threads [max_concurrent=" + max_concurrent + "]");
        long max_sleep = 2000;
        while (!available.isEmpty() || !running.isEmpty()) {
            while ((max_concurrent < 0 || running.size() < max_concurrent) && !available.isEmpty()) {
                R r = available.remove(0);
                Thread thread = (r instanceof Thread ? (Thread)r : new Thread(r));
                thread.start();
                running.add(thread);
                if (debug) {
                    LOG.debug("Started " + thread);
                    LOG.debug("Running=" + running.size() + ", Waiting=" + available.size() + ", Available=" + (max_concurrent - running.size()));
                }
            } // WHILE
            int num_running = running.size();
            long sleep = 10;
            while (num_running > 0) {
                for (int i = 0; i < num_running; i++) {
                    Thread thread = running.get(i);
                    thread.join(sleep);
                    if (!thread.isAlive()) {
                        running.remove(i);
                        if (debug) {
                            LOG.debug(thread + " is complete");
                            LOG.debug("Running=" + running.size() + ", Waiting=" + available.size() + ", Available=" + (max_concurrent - running.size()));
                        }
                        break;
                    }
                } // FOR
                if (num_running != running.size()) break;
                sleep *= 2;
                if (sleep > max_sleep) sleep = max_sleep;
            } // WHILE
        } // WHILE
        if (debug) LOG.debug("All threads are finished");
        return;
    }
    
}
