package edu.brown.api;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.voltdb.client.Client;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Implements the simple state machine for the BenchmarkComponents's remote 
 * controller protocol. This allows the BenchmarkController to control a 
 * BenchmarkComponent. Hypothetically, you can extend this and override the 
 * answerPoll() and answerStart() methods for other clients.
 */
public class ControlPipe implements Runnable {
    private static final Logger LOG = Logger.getLogger(ControlPipe.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug);
    }
    
    final InputStream in;
    final BenchmarkComponent cmp;
    
    /**
     * If this is set to true, then we will not wait for the START command
     * to come from the BenchmarkController. This used for testing/debugging
     */
    private boolean autoStart;
    
    public ControlPipe(BenchmarkComponent component, InputStream in, boolean autoStart) {
        this.cmp = component;
        this.in = in;
        this.autoStart = autoStart;
    }
    
    public void run() {
        final Thread self = Thread.currentThread();
        self.setName(String.format("client-%02d", cmp.getClientId()));

        final boolean profile = cmp.getHStoreConf().client.profiling;
        final Client client = cmp.getClientHandle();
        final Thread workerThread = new Thread(cmp.worker);
        workerThread.setDaemon(true);
        
        // transition to ready and send ready message
        if (cmp.m_controlState == ControlState.PREPARING) {
            cmp.printControlMessage(ControlState.READY);
            cmp.m_controlState = ControlState.READY;
        } else {
            LOG.error("Not starting prepared!");
            LOG.error(cmp.m_controlState + " " + cmp.m_reason);
        }
        
        final BufferedReader in = new BufferedReader(new InputStreamReader(this.in));
        ControlCommand command = null;
        final Pattern p = Pattern.compile(" ");
        while (true) {
            if (this.autoStart) {
                command = ControlCommand.START;
                this.autoStart = false;
            } else {
                try {
                    command = ControlCommand.get(p.split(in.readLine())[0]);
                    if (debug.get()) 
                        LOG.debug(String.format("Recieved Message: '%s'", command));
                } catch (final IOException e) {
                    // Hm. quit?
                    LOG.fatal("Error on standard input", e);
                    System.exit(-1);
                }
            }
            if (command == null) continue;
            if (debug.get()) LOG.debug("ControlPipe Command = " + command);

            // HACK: Convert a SHUTDOWN to a STOP if we're not the first client
            if (command == ControlCommand.SHUTDOWN && cmp.getClientId() != 0) {
                command = ControlCommand.STOP;
            }
            
            switch (command) {
                case START: {
                    if (cmp.m_controlState != ControlState.READY) {
                        cmp.setState(ControlState.ERROR, command + " when not " + ControlState.READY);
                        cmp.answerWithError();
                        continue;
                    }
                    workerThread.start();
                    if (cmp.m_tickThread != null) cmp.m_tickThread.start();
                    cmp.m_controlState = ControlState.RUNNING;
                    cmp.answerOk();
                    break;
                }
                case POLL: {
                    if (cmp.m_controlState != ControlState.RUNNING) {
                        cmp.setState(ControlState.ERROR, command + " when not " + ControlState.RUNNING);
                        cmp.answerWithError();
                        continue;
                    }
                    cmp.answerPoll();
                    
                    // Call tick on the client if we're not polling ourselves
                    if (cmp.m_tickInterval < 0) {
                        if (debug.get()) LOG.debug("Got poll message! Calling tick()!");
                        cmp.invokeTickCallback(cmp.m_tickCounter++);
                    }
                    if (debug.get())
                        LOG.debug(String.format("CLIENT QUEUE TIME: %.2fms / %.2fms avg",
                                                client.getQueueTime().getTotalThinkTimeMS(),
                                                client.getQueueTime().getAverageThinkTimeMS()));
                    break;
                }
                case CLEAR: {
                    cmp.m_txnStats.clear(true);
                    cmp.invokeClearCallback();
                    cmp.answerOk();
                    break;
                }
                case PAUSE: {
                    assert(cmp.m_controlState == ControlState.RUNNING) : "Unexpected " + cmp.m_controlState;
                    if (debug.get()) LOG.debug("Pausing client");
                    
                    // Enable the lock and then change our state
                    try {
                        cmp.m_pauseLock.acquire();
                    } catch (InterruptedException ex) {
                        LOG.fatal("Unexpected interuption!", ex);
                        System.exit(1);
                    }
                    cmp.m_controlState = ControlState.PAUSED;
                    break;
                }
                case SHUTDOWN: {
                    if (debug.get()) LOG.debug("Shutting down client + cluster");
                    if (cmp.m_controlState == ControlState.RUNNING || cmp.m_controlState == ControlState.PAUSED) {
                        cmp.invokeStopCallback();
                        try {
                            client.drain();
                            client.callProcedure("@Shutdown");
                            client.close();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                    System.exit(0);
                    break;
                }
                case STOP: {
                    if (cmp.m_controlState == ControlState.RUNNING || cmp.m_controlState == ControlState.PAUSED) {
                        if (debug.get()) LOG.debug("Stopping client");
                        cmp.invokeStopCallback();
                        
                        if (profile) {
                            System.err.println("ExecuteTime: " + cmp.worker.getExecuteTime().debug(true));
                        }
                        
                        try {
                            if (cmp.m_sampler != null) {
                                cmp.m_sampler.setShouldStop();
                                cmp.m_sampler.join();
                            }
                            client.close();
                            if (cmp.m_checkTables) {
                                cmp.checkTables();
                            }
                        } catch (InterruptedException e) {
                            System.exit(0);
                        } finally {
                            System.exit(0);
                        }
                    }
                    LOG.fatal("STOP when not RUNNING");
                    System.exit(-1);
                    break;
                }
                default: {
                    LOG.fatal("Error on standard input: unknown command " + command);
                    System.exit(-1);
                }
            } // SWITCH
        }
    }

    

}
