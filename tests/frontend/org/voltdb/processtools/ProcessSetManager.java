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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.voltdb.utils.Pair;

import edu.brown.benchmark.BenchmarkComponent.Command;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.FileUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;
import edu.brown.hstore.interfaces.Shutdownable;

public class ProcessSetManager implements Shutdownable {
    private static final Logger LOG = Logger.getLogger(ProcessSetManager.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private static final SimpleDateFormat BACKUP_FORMAT = new SimpleDateFormat("yyyy.MM.dd-HH:mm:ss");
    
    final int initial_polling_delay; 
    final File output_directory;
    final EventObservable<String> failure_observable = new EventObservable<String>();
    final LinkedBlockingQueue<OutputLine> m_output = new LinkedBlockingQueue<OutputLine>();
    final Map<String, ProcessData> m_processes = new ConcurrentHashMap<String, ProcessData>();
    final ProcessSetPoller setPoller = new ProcessSetPoller();
    boolean shutting_down = false;
    boolean backup_logs = true;
    
    
    public static final Pattern OUTPUT_CLEAN[] = {
        Pattern.compile("__(FILE|LINE)__[:]?"),
        Pattern.compile("[A-Z][\\w\\_]+\\.java:[\\d]+ ")
    };
    
    public enum Stream { STDERR, STDOUT; }

    static class ProcessData {
        Process process;
        ProcessPoller poller;
        int pid;
        StreamWatcher out;
        StreamWatcher err;
    }

    /**
     *
     *
     */
    public final class OutputLine {
        OutputLine(String processName, Stream stream, String value) {
            assert(value != null);
            this.processName = processName;
            this.stream = stream;
            this.value = value;
        }

        public final String processName;
        public final Stream stream;
        public final String value;
        
        @Override
        public String toString() {
            return String.format("{%s, %s, \"%s\"}", processName, stream, value);
        }
    }

    static Set<Process> createdProcesses = new HashSet<Process>();
    static class ShutdownThread extends Thread {
        @Override
        public void run() {
            synchronized(createdProcesses) {
                for (Process p : createdProcesses)
                    p.destroy();
            }
        }
    }
    static {
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());
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
                    if (shutting_down == false && debug.get()) {
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
        
        ProcessSetPoller() {
            this.setDaemon(true);
            this.setPriority(MIN_PRIORITY);
        }
        @Override
        public void run() {
            if (debug.get())
                LOG.debug("Starting ProcessSetPoller [initialDelay=" + initial_polling_delay + "]");
            while (true) {
                try {
                    Thread.sleep(2500);
                } catch (InterruptedException ex) {
                    if (shutting_down == false) ex.printStackTrace();
                    break;
                }
                for (Entry<String, ProcessData> e : m_processes.entrySet()) {
                    ProcessData pd = e.getValue();
                    if (pd.poller == null) continue;
                    
                    Boolean isAlive = pd.poller.isProcessAlive();
                    if (isAlive == null) continue;
                    if (isAlive == false && reported_error == false && isShuttingDown() == false) {
                        String msg = String.format("Failed to poll '%s'", e.getKey());
                        LOG.error(msg);
                        
                        msg = String.format("Process '%s' failed. Halting benchmark!", e.getKey());
                        failure_observable.notifyObservers(msg);
                        reported_error = true;
                    }
                } // FOR
            } // WHILE
        }
    } // END CLASS
    
    class StreamWatcher extends Thread {
        final BufferedReader m_reader;
        final String m_processName;
        final Stream m_stream;
        final AtomicBoolean m_expectDeath = new AtomicBoolean(false);
        final FileWriter m_writer;

        StreamWatcher(BufferedReader reader, FileWriter writer, String processName, Stream stream) {
            assert(reader != null);
            this.setDaemon(true);
            m_reader = reader;
            m_writer = writer;
            m_processName = processName;
            m_stream = stream;
        }

        void setExpectDeath(boolean expectDeath) {
            m_expectDeath.set(expectDeath);
        }

        @Override
        public void run() {
            try {
                while (true) {
                    String line = null;
                    try {
                        line = m_reader.readLine();
                    } catch (IOException e) {
                        if (!m_expectDeath.get()) {
                            synchronized (ProcessSetManager.this) { 
                                if (shutting_down == false)
                                    LOG.error(String.format("Stream monitoring thread for '%s' is exiting", m_processName), (debug.get() ? e : null));
                                failure_observable.notifyObservers(m_processName);
                            } // SYNCH
                        }
                        return;
                    }
                    
                    // HACK: Remove stuff that we don't want printed
                    if (line != null && line.isEmpty() == false) {
                        for (Pattern p : OUTPUT_CLEAN) {
                            Matcher m = p.matcher(line);
                            if (m != null) line = m.replaceAll("");
                        }
                    }

                    if (line != null) {
                        OutputLine ol = new OutputLine(m_processName, m_stream, line);
                        // final long now = (System.currentTimeMillis() / 1000) - 1256158053;
                        // m_writer.write(String.format("(%d) %s: %s\n", now, m_processName, line));
                        if (m_writer != null) {
                            synchronized (m_writer) {
                                m_writer.write(line + "\n");
                                m_writer.flush();
                            } // SYNCH
                        }
                        m_output.add(ol);
                    }
                    else {
                        Thread.yield();
                        if (m_writer != null) m_writer.flush();
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        }
    }
    
    public ProcessSetManager(String log_dir, boolean backup_logs, int initial_polling_delay, EventObserver<String> observer) {
        this.output_directory = (log_dir != null && log_dir.isEmpty() == false ? new File(log_dir) : null);
        this.backup_logs = backup_logs;
        this.initial_polling_delay = initial_polling_delay;
        this.failure_observable.addObserver(observer);
        
        if (this.output_directory != null)
            FileUtil.makeDirIfNotExists(this.output_directory.getAbsolutePath());
    }
    
    public ProcessSetManager() {
        this(null, false, 10000, null);
    }
    
    @Override
    public synchronized void prepareShutdown(boolean error) {
        this.shutting_down = true;
        for (String name : this.m_processes.keySet()) {
            ProcessData pd = this.m_processes.get(name);
            assert(pd!= null) : "Invalid process name '" + name + "'";
            pd.out.m_expectDeath.set(true);
            pd.err.m_expectDeath.set(true);
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
            if (debug.get())
                LOG.debug(String.format("Killing %d processes in parallel", runnables.size()));
            try {
                ThreadUtil.runNewPool(runnables);
            } catch (Throwable ex) {
                LOG.error("Unexpected error when shutting down processes", ex);
            }
            if (debug.get())
                LOG.debug("Finished shutting down");
        }
    }
    
    @Override
    public synchronized boolean isShuttingDown() {
        return (this.shutting_down);
    }

    public String[] getProcessNames() {
        String[] retval = new String[m_processes.size()];
        int i = 0;
        for (String clientName : m_processes.keySet())
            retval[i++] = clientName;
        return retval;
    }

    public void startProcess(String processName, String[] cmd) {
        if (debug.get()) LOG.debug("Starting Process: " + StringUtil.join(" ", cmd));
        
        ProcessBuilder pb = new ProcessBuilder(cmd);
        ProcessData pd = new ProcessData();
        try {
            pd.process = pb.start();
            synchronized (createdProcesses) {
                createdProcesses.add(pd.process);
                assert(m_processes.containsKey(processName) == false) : processName + "\n" + m_processes;
                m_processes.put(processName, pd);
                
                // Start the individual watching thread for this process
                pd.poller = new ProcessPoller(pd.process, processName);
                pd.poller.start();
                
                if (this.setPoller.isAlive() == false) this.setPoller.start();
            } // SYNCH
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
//        Pair<Integer, Process> pair = ThreadUtil.exec(cmd);
//        ProcessData pd = new ProcessData();
//        pd.pid = pair.getFirst();
//        pd.process = pair.getSecond();
//        createdProcesses.add(pd.process);
//        assert(m_processes.containsKey(processName) == false) : processName + "\n" + m_processes;
//        m_processes.put(processName, pd);
        
        BufferedReader out = new BufferedReader(new InputStreamReader(pd.process.getInputStream()));
        BufferedReader err = new BufferedReader(new InputStreamReader(pd.process.getErrorStream()));
        
        // Output File
        FileWriter fw = null;
        if (output_directory != null) {
            String baseName = String.format("%s/%s.log", output_directory.getAbsolutePath(), processName);
            File path = new File(baseName);
            
            // 2012-01-24
            // If the file already exists, we'll move it out of the way automatically 
            // if they want us to
            if (path.exists() && backup_logs) {
                Date log_date = new Date(path.lastModified());
                File backup_file = new File(baseName + "-" + BACKUP_FORMAT.format(log_date));
                path.renameTo(backup_file);
                if (debug.get())
                    LOG.debug(String.format("Moved log file '%s' to '%s'", path.getName(), backup_file.getName())); 
            }
            
            try {
                fw = new FileWriter(path);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to create output writer for " + processName, ex);
            }
            if (debug.get()) 
                LOG.debug(String.format("Logging %s output to '%s'", processName, path));
        }
        
        pd.out = new StreamWatcher(out, fw, processName, Stream.STDOUT);
        pd.err = new StreamWatcher(err, fw, processName, Stream.STDERR);
        
        pd.out.start();
        pd.err.start();
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

    public void writeToAll(Command cmd) {
        LOG.debug(String.format("Sending %s to all processes", cmd));
        for (String processName : m_processes.keySet()) {
            this.writeToProcess(processName, cmd + "\n");
        }
    }
    
    public void writeToProcess(String processName, Command cmd) {
        this.writeToProcess(processName, cmd + "\n");
    }
    
    public void writeToProcess(String processName, String data) {
        ProcessData pd = m_processes.get(processName);
        assert(pd != null);
        OutputStreamWriter out = new OutputStreamWriter(pd.process.getOutputStream());
        try {
            out.write(data);
            out.flush();
        } catch (IOException e) {
            if (processName.contains("client-")) return;
            synchronized (this) {
                if (this.shutting_down == false) {
                    String msg = "";
                    if (data.trim().isEmpty()) {
                        msg = String.format("Failed to poll '%s'", processName);
                    } else {
                        msg = String.format("Failed to write '%s' command to '%s'", data.trim(), processName);
                    }
                    if (LOG.isDebugEnabled()) LOG.fatal(msg, e);
                    else LOG.fatal(msg);
                }
            } // SYNCH
            this.failure_observable.notifyObservers(processName);
        }
    }

    public synchronized Map<String, Integer> joinAll() {
        Map<String, Integer> retvals = new HashMap<String, Integer>();
        Long wait = 5000l;
        for (String processName : m_processes.keySet()) {
            Pair<Integer, Boolean> p = this.joinProcess(processName, wait);
            if (p.getSecond() && wait != null) {
                wait /= 2;
                if (wait < 1) wait = null; 
            }
            retvals.put(processName, p.getFirst());
        } // FOR
        return (retvals);
    }
    
    public int joinProcess(String processName) {
        return joinProcess(processName, null).getFirst();
    }
    
    public Pair<Integer, Boolean> joinProcess(String processName, Long millis) {
        final ProcessData pd = m_processes.get(processName);
        assert(pd != null);
        pd.out.m_expectDeath.set(true);
        pd.err.m_expectDeath.set(true);

        final CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread() {
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
            if (millis != null)
                timeout = (latch.await(millis, TimeUnit.MILLISECONDS) == false);
            else
                latch.await();
        } catch (InterruptedException ex) {
            // Ignore...
        }
        
        int retval = killProcess(processName); 
        return Pair.of(retval, timeout);
    }

    public int killProcess(String processName) {
        ProcessData pd = m_processes.get(processName);
        if (pd != null) {
            if (pd.out != null) pd.out.m_expectDeath.set(true);
            if (pd.err != null) pd.err.m_expectDeath.set(true);
        }
        int retval = -255;

        if (debug.get()) LOG.debug("Killing '" + processName + "'");
        pd.process.destroy();
        try {
            pd.process.waitFor();
            retval = pd.process.exitValue();
        } catch (InterruptedException e) {
            synchronized (this) {
                if (this.shutting_down == false) e.printStackTrace();
            } // SYNCH
        }

        synchronized(createdProcesses) {
            createdProcesses.remove(pd.process);
            pd.poller.interrupt();
        }

        return retval;
    }

    public int size() {
        return m_processes.size();
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
