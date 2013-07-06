/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.processtools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Pair;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.FileUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;
import edu.brown.interfaces.Shutdownable;

public class ProcessSetManager implements Shutdownable {
    private static final Logger LOG = Logger.getLogger(ProcessSetManager.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private static final SimpleDateFormat BACKUP_FORMAT = new SimpleDateFormat("yyyy.MM.dd-HH:mm:ss");
    
    /**
     * How long to wait after a process starts before we will check whether
     * it's still alive. 
     */
    private static final int POLLING_DELAY = 2000; // ms
    
    /**
     * Regular expressions of strings that we want to exclude from the remote
     * process' output. This is just to make the debug logs easier to read.  
     */
    public static final Pattern OUTPUT_CLEAN[] = {
        Pattern.compile("__(FILE|LINE)__[:]?"),
        Pattern.compile("[A-Z][\\w\\_]+\\.java:[\\d]+ "),
        Pattern.compile("^[\\s]+\\[java\\] ")
    };
    
    public enum StreamType { STDERR, STDOUT; }
    

    private static class ProcessData {
        final Process process;
        final OutputStreamWriter out;
        ProcessPoller poller;
        StreamWatcher stdout;
        StreamWatcher stderr;
        
        ProcessData(Process process) {
            this.process = process;
            this.out = new OutputStreamWriter(this.process.getOutputStream());
        }
    }

    /**
     *
     *
     */
    public final class OutputLine {
        OutputLine(String processName, StreamType stream, String value) {
            assert(value != null);
            this.processName = processName;
            this.stream = stream;
            this.value = value;
        }

        public final String processName;
        public final StreamType stream;
        public final String value;
        
        @Override
        public String toString() {
            return String.format("{%s, %s, \"%s\"}", processName, stream, value);
        }
    }

    class ProcessPoller extends Thread {
        final Process p;
        final String name;
        Boolean is_alive = null;
        
        public ProcessPoller(Process p, String name) {
            this.p = p;
            this.name = name;
            this.setDaemon(true);
            this.setPriority(MIN_PRIORITY);
        }
        @Override
        public void run() {
            try {
                this.is_alive = true;
                this.p.waitFor();
            } catch (InterruptedException ex) {
                // IGNORE
            } finally {
                synchronized (ProcessSetManager.this) {
                    if (debug.val && shutting_down == false) {
                        String msg = String.format("'%s' has stopped [wasAlive=%s]", this.name, this.is_alive);
                        LOG.warn(msg);
                    }
                } // SYNCH
                this.is_alive = false;
            }
        }
        public Boolean isProcessAlive() {
            return (this.is_alive);
        }
    } // END CLASS
    
    class ProcessSetPoller extends Thread {
        boolean reported_error = false;
        final Map<String, Long> delay = new HashMap<String, Long>();
        
        ProcessSetPoller() {
            this.setDaemon(true);
            this.setPriority(MIN_PRIORITY);
        }
        @Override
        public void run() {
            if (debug.val)
                LOG.debug("Starting ProcessSetPoller [initialDelay=" + initial_polling_delay + "]");
            final Set<String> toPoll = new HashSet<String>(); 
            while (true) {
                try {
                    Thread.sleep(2500);
                } catch (InterruptedException ex) {
                    if (shutting_down == false) ex.printStackTrace();
                    break;
                }
                // First figure out what processes that we want to poll
                // If we have a new entry, then we will want to wait a little bit to make
                // sure that it comes on-line
                toPoll.clear();
                long timestamp = System.currentTimeMillis();
                for (Entry<String, ProcessData> e : m_processes.entrySet()) {
                    // This is the first time that that we've seen it
                    if (this.delay.containsKey(e.getKey()) == false) {
                        this.delay.put(e.getKey(), timestamp + POLLING_DELAY);
                        if (debug.val) LOG.debug(String.format("Waiting %.1f seconds before polling '%s'",
                                                   POLLING_DELAY/1000d, e.getKey()));
                    }
                    // Otherwise, check whether the time has elapsed
                    else if (timestamp > this.delay.get(e.getKey())) {
                        toPoll.add(e.getKey());
                    }
                } // FOR
                
                for (String procName : toPoll) {
                    ProcessData pd = m_processes.get(procName);
                    if (pd.poller == null) continue;
                    Boolean isAlive = pd.poller.isProcessAlive();
                    if (isAlive == null) continue;
                    if (isAlive == false && reported_error == false && isShuttingDown() == false) {
                        String msg = String.format("Failed to poll '%s' [exitValue=%d]", procName, pd.process.exitValue());
                        LOG.error(msg);
                        msg = String.format("Process '%s' failed. Halting benchmark!", procName);
                        failure_observable.notifyObservers(msg);
                        reported_error = true;
                    }
                } // FOR
            } // WHILE
        }
    } // END CLASS
    
    /**
     * Thread that polls the given BufferedReader and parses its output.
     * The contents are written to the FileWriter and added to the output queue for
     * further processing
     */
    class StreamWatcher extends Thread {
        final BufferedReader reader;
        final String processName;
        final StreamType streamType;
        final AtomicBoolean expectDeath = new AtomicBoolean(false);
        final AtomicBoolean shutdownMsg = new AtomicBoolean(false);
        final FileWriter writer;

        StreamWatcher(BufferedReader reader, FileWriter writer, String processName, StreamType streamType) {
            assert(reader != null);
            this.setDaemon(true);
            this.reader = reader;
            this.writer = writer;
            this.processName = processName;
            this.streamType = streamType;
        }

        void setExpectDeath(boolean expectDeath) {
            this.expectDeath.set(expectDeath);
        }
        
        void shutdown(Throwable error) {
            if (this.expectDeath.get()) return;
            
            if (ProcessSetManager.this.shutting_down == false && this.shutdownMsg.compareAndSet(false, true)) {
                String msg = String.format("Stream monitoring thread for '%s' %s is exiting",
                                           this.processName, this.streamType); 
                LOG.error(msg, (debug.val ? error : null));
                ProcessSetManager.this.failure_observable.notifyObservers(this.processName);
            }
        }

        @Override
        public void run() {
            try {
                while (true) {
                    String line = null;
                    try {
                        line = this.reader.readLine();
                    } catch (IOException e) {
                        this.shutdown(e);
                        return;
                    }
                    
                    // Skip empty input
                    if (line == null || line.isEmpty()) {
                        Thread.yield();
                        if (this.writer != null) this.writer.flush();
                        continue;
                    }
                        
                    // Remove stuff that we don't want printed
                    for (Pattern p : OUTPUT_CLEAN) {
                        Matcher m = p.matcher(line);
                        if (m != null) line = m.replaceAll("");
                    } // FOR

                    // Otherwise parse it so that somebody else can process it 
                    OutputLine ol = new OutputLine(this.processName, this.streamType, line);
                    if (this.writer != null) {
                        synchronized (this.writer) {
                            this.writer.write(line + "\n");
                            this.writer.flush();
                        } // SYNCH
                    }
                    ProcessSetManager.this.m_output.add(ol);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        }
    }
    
    // ============================================================================
    // INSTANCE MEMBERS
    // ============================================================================
    
    final int initial_polling_delay; 
    final File output_directory;
    final EventObservable<String> failure_observable = new EventObservable<String>();
    final LinkedBlockingQueue<OutputLine> m_output = new LinkedBlockingQueue<OutputLine>();
    final Map<String, ProcessData> m_processes = new ConcurrentHashMap<String, ProcessData>();
    final ProcessSetPoller setPoller = new ProcessSetPoller();
    boolean shutting_down = false;
    boolean backup_logs = true;

    // ============================================================================
    // INITIALIZATION
    // ============================================================================

    /**
     * Constructor
     * @param log_dir
     * @param backup_logs
     * @param initial_polling_delay
     * @param failureObserver
     */
    public ProcessSetManager(String log_dir, boolean backup_logs, int initial_polling_delay, EventObserver<String> failureObserver) {
        this.output_directory = (log_dir != null && log_dir.isEmpty() == false ? new File(log_dir) : null);
        this.backup_logs = backup_logs;
        this.initial_polling_delay = initial_polling_delay;
        this.failure_observable.addObserver(failureObserver);
        
        if (this.output_directory != null)
            FileUtil.makeDirIfNotExists(this.output_directory.getAbsolutePath());
    }
    
    public ProcessSetManager() {
        this(null, false, 10000, null);
    }
    
    // ============================================================================
    // SHUTDOWN METHODS
    // ============================================================================

    /**
     * A list of all the processes that we have ever created
     * This is just to give us an extra chance to remove anything that we may have
     * left laying around if we fail to shutdown cleanly.  
     */
    private static Collection<Process> ALL_PROCESSES = new HashSet<Process>();
    private static class ShutdownThread extends Thread {
        @Override
        public void run() {
            synchronized(ProcessSetManager.class) {
                for (Process p : ALL_PROCESSES)
                    p.destroy();
            } // SYNCH
        }
    }
    static {
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());
    }
    
    
    @Override
    public synchronized void prepareShutdown(boolean error) {
        this.shutting_down = true;
        for (String name : this.m_processes.keySet()) {
            ProcessData pd = this.m_processes.get(name);
            assert(pd!= null) : "Invalid process name '" + name + "'";
            pd.stdout.expectDeath.set(true);
            pd.stderr.expectDeath.set(true);
        } // FOR
    }
    
    @Override
    public synchronized void shutdown() {
        this.shutting_down = true;
        this.setPoller.interrupt();
        List<Runnable> runnables = new ArrayList<Runnable>(); 
        for (final String name : m_processes.keySet()) {
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    killProcess(name);
                }
            });
        } // FOR
        if (runnables.isEmpty() == false) {
            if (debug.val)
                LOG.debug(String.format("Killing %d processes in parallel", runnables.size()));
            try {
                ThreadUtil.runNewPool(runnables);
            } catch (Throwable ex) {
                LOG.error("Unexpected error when shutting down processes", ex);
            }
            if (debug.val)
                LOG.debug("Finished shutting down");
        }
    }

    @Override
    public synchronized boolean isShuttingDown() {
        return (this.shutting_down);
    }

    // ============================================================================
    // UTILITY METHODS
    // ============================================================================
    
    public String[] getProcessNames() {
        String[] retval = new String[m_processes.size()];
        int i = 0;
        for (String clientName : m_processes.keySet())
            retval[i++] = clientName;
        return retval;
    }

    public int size() {
        return m_processes.size();
    }
    
    public OutputLine nextBlocking() {
        try {
            return m_output.take();
        } catch (InterruptedException e) {
            // if (this.shutting_down == false) e.printStackTrace();
        }
        return null;
    }

    public OutputLine nextNonBlocking() {
        return m_output.poll();
    }

    public void writeToAll(String cmd) {
        if (debug.val) LOG.debug(String.format("Sending %s to all processes", cmd));
        for (String processName : m_processes.keySet()) {
            this.writeToProcess(processName, cmd + "\n");
        }
    }
    
    public void writeToProcess(String processName, String data) {
        if (debug.val) LOG.debug(String.format("Writing '%s' to process %s", data.trim(), processName));
        
        // You always need a newline at the end of it to ensure that 
        // it flushes properly
        if (data.endsWith("\n") == false) data += "\n";
        
        ProcessData pd = m_processes.get(processName);
        assert(pd != null) :
            "Missing ProcessData for '" + processName + "'";
        assert(pd.out != null) :
            "Missing OutputStreamWriter for '" + processName + "'";
        try {
            pd.out.write(data);
            pd.out.flush();
        } catch (IOException e) {
            if (processName.contains("client-")) return; // FIXME
            synchronized (this) {
                if (this.shutting_down == false) {
                    String msg = "";
                    if (data.trim().isEmpty()) {
                        msg = String.format("Failed to poll '%s'", processName);
                    } else {
                        msg = String.format("Failed to write '%s' command to '%s'", data.trim(), processName);
                    }
                    if (LOG.isDebugEnabled()) LOG.fatal(msg, e);
                    else LOG.fatal(msg, e);
                }
            } // SYNCH
            this.failure_observable.notifyObservers(processName);
        }
    }

    // ============================================================================
    // START PROCESS
    // ============================================================================

    /**
     * Main method for starting a new process under this manager
     * The given processName must be a unique handle that we will us to
     * identify this new process in the future.  
     * @param processName
     * @param cmd
     */
    public void startProcess(String processName, String[] cmd) {
        if (debug.val) LOG.debug("Starting Process: " + StringUtil.join(" ", cmd));
        
        ProcessBuilder pb = new ProcessBuilder(cmd);
        ProcessData pd = null;
        try {
            synchronized (ProcessSetManager.class) {
                if (m_processes.containsKey(processName)) {
                    throw new RuntimeException("Duplicate process name '" + processName + "'");
                }
                pd = new ProcessData(pb.start());
                m_processes.put(processName, pd);
                ALL_PROCESSES.add(pd.process);
            } // SYNCH
        } catch (IOException e) {
            throw new RuntimeException("Failed to start process '" + processName + "'", e);
        }
        
        BufferedReader out = new BufferedReader(new InputStreamReader(pd.process.getInputStream()));
        BufferedReader err = new BufferedReader(new InputStreamReader(pd.process.getErrorStream()));
        
        // Output File
        // We use a single output file for stdout and stderr
        FileWriter fw = null;
        if (this.output_directory != null) {
            String baseName = String.format("%s/%s.log", this.output_directory.getAbsolutePath(), processName);
            File path = new File(baseName);
            
            // 2012-01-24
            // If the file already exists, we'll move it out of the way automatically 
            // if they want us to
            if (path.exists() && this.backup_logs) {
                Date log_date = new Date(path.lastModified());
                File backup_file = new File(baseName + "-" + BACKUP_FORMAT.format(log_date));
                path.renameTo(backup_file);
                if (debug.val)
                    LOG.debug(String.format("Moved log file '%s' to '%s'", path.getName(), backup_file.getName())); 
            }
            
            try {
                fw = new FileWriter(path);
                fw.write("# " + new TimestampType().toString() + "\n");
            } catch (Exception ex) {
                throw new RuntimeException("Failed to create output writer for " + processName, ex);
            }
            if (debug.val) 
                LOG.debug(String.format("Logging %s output to '%s'", processName, path));
        }
        
        pd.stdout = new StreamWatcher(out, fw, processName, StreamType.STDOUT);
        pd.stderr = new StreamWatcher(err, fw, processName, StreamType.STDERR);
        
        pd.stdout.start();
        pd.stderr.start();
        
        // Start the individual watching thread for this process
        pd.poller = new ProcessPoller(pd.process, processName);
        pd.poller.start();
        synchronized (this) {
            if (this.setPoller.isAlive() == false) this.setPoller.start();
        } // SYNCH
    }

    // ============================================================================
    // STOP PROCESSES
    // ============================================================================
    
    public synchronized Map<String, Integer> joinAll() {
        Map<String, Integer> retvals = new HashMap<String, Integer>();
        Long wait = 5000l;
        for (String processName : m_processes.keySet()) {
            Pair<Integer, Boolean> p = this.joinProcess(processName, wait);
            if (p.getSecond() && wait != null) {
                wait = (long)Math.ceil(wait.longValue() * 0.75);
                // if (wait < 1) wait = 1l; 
            }
            retvals.put(processName, p.getFirst());
        } // FOR
        return (retvals);
    }
    
    public int joinProcess(String processName) {
        return this.joinProcess(processName, null).getFirst();
    }
    
    /**
     * Returns the status code + flag if we timed out
     * @param processName
     * @param millis
     * @return
     */
    public Pair<Integer, Boolean> joinProcess(String processName, Long millis) {
        final ProcessData pd = m_processes.get(processName);
        assert(pd != null);
        pd.stdout.expectDeath.set(true);
        pd.stderr.expectDeath.set(true);

        final CountDownLatch latch = new CountDownLatch(1);
        final String name = String.format("%s Killer [%s]", this.getClass().getSimpleName(), processName); 
        Thread t = new Thread(name) {
            public void run() {
                try {
                    pd.process.waitFor();
                } catch (InterruptedException e) {
                    synchronized (this) {
                        if (shutting_down == false) e.printStackTrace();
                    } // SYNCH
                }
                latch.countDown();
            }
        };
        t.setDaemon(true);
        t.start();
        
        boolean timeout = false;
        try {
            if (millis != null) {
                timeout = (latch.await(millis, TimeUnit.MILLISECONDS) == false);
            } else {
                latch.await();
            }
        } catch (InterruptedException ex) {
            // Ignore...
        }
        
        int retval = this.killProcess(processName); 
        return Pair.of(retval, timeout);
    }

    /**
     * Forcibly kill the process and return its exit code
     * @param processName
     * @return
     */
    public int killProcess(String processName) {
        ProcessData pd = m_processes.get(processName);
        if (pd != null) {
            if (pd.stdout != null) pd.stdout.expectDeath.set(true);
            if (pd.stderr != null) pd.stderr.expectDeath.set(true);
        }
        int retval = -255;

        if (debug.val) LOG.debug("Killing '" + processName + "'");
        pd.process.destroy();
        try {
            pd.process.waitFor();
            retval = pd.process.exitValue();
        } catch (InterruptedException e) {
            synchronized (this) {
                if (this.shutting_down == false) e.printStackTrace();
            } // SYNCH
        }

        synchronized(ProcessSetManager.class) {
            ALL_PROCESSES.remove(pd.process);
            pd.poller.interrupt();
        } // SYNCH

        return retval;
    }
    
    public static void main(String[] args) {
        ProcessSetManager psm = new ProcessSetManager();
        psm.startProcess("ping4c", new String[] { "ping", "volt4c" });
        psm.startProcess("ping3c", new String[] { "ping", "volt3c" });
        while(true) {
            OutputLine line = psm.nextBlocking();
            System.out.printf("(%s:%s): %s\n", line.processName, line.stream.name(), line.value);
        }
    }

}
