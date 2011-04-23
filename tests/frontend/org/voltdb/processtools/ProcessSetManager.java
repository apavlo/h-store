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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.voltdb.benchmark.ClientMain.Command;

import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;

public class ProcessSetManager {
    private static final Logger LOG = Logger.getLogger(ProcessSetManager.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    
    final File output_directory;
    final EventObservable failure_observable = new EventObservable();
    final LinkedBlockingQueue<OutputLine> m_output = new LinkedBlockingQueue<OutputLine>();
    final Map<String, ProcessData> m_processes = new HashMap<String, ProcessData>();
    final Map<String, StreamWatcher> m_watchers = new HashMap<String, StreamWatcher>();
    final ProcessSetPoller poller = new ProcessSetPoller();
    boolean shutting_down = false;
    
    public enum Stream { STDERR, STDOUT; }

    static class ProcessData {
        Process process;
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

    class ProcessSetPoller extends Thread {
        {
            this.setDaemon(true);
            this.setPriority(MIN_PRIORITY);
        }
        
        @Override
        public void run() {
            LOG.debug("Starting ProcessSetPoller");
            while (true) {
                try {
                    Thread.sleep(2500);
                } catch (InterruptedException ex) {
                    if (shutting_down == false) ex.printStackTrace();
                    break;
                }
                for (Entry<String, ProcessData> e : m_processes.entrySet()) {
                    ProcessData pd = e.getValue();
                    if (pd.err != null && pd.err.isAlive()) {
                        LOG.debug("Polling " + e.getKey());
                        writeToProcess(e.getKey(), " ");
                    }
                } // FOR
            } // WHILE
        }
    }
    
    class StreamWatcher extends Thread {
        final BufferedReader m_reader;
        final String m_processName;
        final Stream m_stream;
        final AtomicBoolean m_expectDeath = new AtomicBoolean(false);
        final FileWriter m_writer;

        StreamWatcher(BufferedReader reader, String processName, Stream stream) {
            assert(reader != null);
            this.setDaemon(true);
            m_reader = reader;
            m_processName = processName;
            m_stream = stream;
            
            if (output_directory != null) {
                FileWriter fw = null;
                String path = output_directory.getAbsolutePath() + "/hstore-" + m_processName;
                try {
                    fw = new FileWriter(path);
                } catch (Exception ex) {
                    LOG.fatal("Failed to create output writer for " + m_processName, ex);
                    System.exit(1);
                }
                if (debug.get()) LOG.debug(String.format("Logging %s output to '%s'", m_processName, path));
                m_writer = fw;
            } else {
                m_writer = null;
            }
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
                            if (shutting_down == false)
                                LOG.error(String.format("Stream monitoring thread for '%s' is exiting", m_processName), e);
                            failure_observable.notifyObservers(m_processName);
                        }
                        return;
                    }

                    if (line != null) {
                        OutputLine ol = new OutputLine(m_processName, m_stream, line);
                        m_output.add(ol);
                        // final long now = (System.currentTimeMillis() / 1000) - 1256158053;
                        // m_writer.write(String.format("(%d) %s: %s\n", now, m_processName, line));
                        if (m_writer != null) {
                            m_writer.write(line + "\n");
                            m_writer.flush();
                        }
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
    
    public ProcessSetManager(String log_dir, EventObserver observer) {
        this.output_directory = (log_dir != null ? new File(log_dir) : null);
        this.failure_observable.addObserver(observer);
    }
    
    public ProcessSetManager() {
        this(null, null);
    }
    
    public void prepareToShutdown() {
        this.shutting_down = true;
    }

    public String[] getProcessNames() {
        String[] retval = new String[m_processes.size()];
        int i = 0;
        for (String clientName : m_processes.keySet())
            retval[i++] = clientName;
        return retval;
    }

    public void startProcess(String processName, String[] cmd) {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        ProcessData pd = new ProcessData();
        try {
            pd.process = pb.start();
            synchronized (createdProcesses) {
                createdProcesses.add(pd.process);
                assert(m_processes.containsKey(processName) == false) : processName + "\n" + m_processes;
                m_processes.put(processName, pd);
                
                if (this.poller.isAlive() == false) this.poller.start();
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
        pd.out = new StreamWatcher(out, processName, Stream.STDOUT);
        pd.err = new StreamWatcher(err, processName, Stream.STDERR);
        
        pd.out.start();
        pd.err.start();
    }

    public OutputLine nextBlocking() {
        try {
            return m_output.take();
        } catch (InterruptedException e) {
            if (this.shutting_down == false) e.printStackTrace();
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
            if (this.shutting_down == false) {
                String msg = "";
                if (data.trim().isEmpty()) {
                    msg = String.format("Failed to poll '%s'", processName);
                } else {
                    msg = String.format("Failed to write '%s' command to '%s'", data.trim(), processName);
                }
                LOG.fatal(msg, e);
            }
            this.failure_observable.notifyObservers(processName);
        }
    }

    public int joinProcess(String processName) {
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
                    if (shutting_down == false) e.printStackTrace();
                }
                latch.countDown();
            }
        };
        t.setDaemon(true);
        t.start();
        
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            // Ignore...
        }
        return killProcess(processName);
    }

    public int killProcess(String processName) {
        ProcessData pd = m_processes.get(processName);
        pd.out.m_expectDeath.set(true);
        pd.err.m_expectDeath.set(true);
        int retval = -255;

        pd.process.destroy();
        try {
            pd.process.waitFor();
            retval = pd.process.exitValue();
        } catch (InterruptedException e) {
            if (this.shutting_down == false) e.printStackTrace();
        }

        synchronized(createdProcesses) {
            createdProcesses.remove(pd.process);
        }

        return retval;
    }

    public void killAll() {
        this.shutting_down = true;
        poller.interrupt();
        for (String name : m_processes.keySet()) {
            killProcess(name);
        }
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
