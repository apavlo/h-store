package edu.brown.api;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.client.Client;
import org.voltdb.sysprocs.Shutdown;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.ProfileMeasurement;
import edu.brown.profilers.ProfileMeasurementUtil;

/**
 * Implements the simple state machine for the BenchmarkComponents's remote 
 * controller protocol. This allows the BenchmarkController to control a 
 * BenchmarkComponent. Hypothetically, you can extend this and override the 
 * answerPoll() and answerStart() methods for other clients.
 */
public class ControlPipe implements Runnable {
    private static final Logger LOG = Logger.getLogger(ControlPipe.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug);
    }
    
    private final InputStream in;
    private final BenchmarkComponent cmp;
    private boolean stop = false;
    
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

        boolean profile = cmp.getHStoreConf().client.profiling;
        if (profile) cmp.worker.enableProfiling(true);
        
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
        final Pattern p = Pattern.compile(" ");
        ControlCommand command = null;
        while (this.stop == false) {
            if (this.autoStart) {
                command = ControlCommand.START;
                this.autoStart = false;
            } else {
                try {
                    command = ControlCommand.get(p.split(in.readLine())[0]);
                    if (debug.val) 
                        LOG.debug(String.format("Recieved Message: '%s'", command));
                } catch (final IOException e) {
                    // Hm. quit?
                    throw new RuntimeException("Error on standard input", e);
                }
            }
            if (command == null) continue;
            if (debug.val) LOG.debug("ControlPipe Command = " + command);

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
                    
                    // Dump Profiling Info
                    if (profile) System.err.println(this.getProfileInfo());
                    
                    // Call tick on the client if we're not polling ourselves
                    if (cmp.m_tickInterval < 0) {
                        if (debug.val) LOG.debug("Got poll message! Calling tick()!");
                        cmp.invokeTickCallback(cmp.m_tickCounter++);
                    }
                    if (debug.val)
                        LOG.debug(String.format("CLIENT QUEUE TIME: %.2fms / %.2fms avg",
                                                client.getQueueTime().getTotalThinkTimeMS(),
                                                client.getQueueTime().getAverageThinkTimeMS()));
                    break;
                }
                case DUMP_TXNS: {
                    if (cmp.m_controlState != ControlState.PAUSED) {
                        cmp.setState(ControlState.ERROR, command + " when not " + ControlState.PAUSED);
                        cmp.answerWithError();
                        continue;
                    }
                    if (debug.val) LOG.debug("DUMP TRANSACTIONS!");
                    cmp.answerDumpTxns();
                    break;
                }
                case CLEAR: {
                    cmp.invokeClearCallback();
                    cmp.answerOk();
                    break;
                }
                case PAUSE: {
                    assert(cmp.m_controlState == ControlState.RUNNING) : "Unexpected " + cmp.m_controlState;
                    if (debug.val) LOG.debug("Pausing client " + cmp.getClientId());
                    
                    // Enable the lock and then change our state
                    try {
                        if (debug.val) LOG.debug("Acquiring pause lock");
                        cmp.m_pauseLock.acquire();
                    } catch (InterruptedException ex) {
                        LOG.fatal("Unexpected interuption!", ex);
                        throw new RuntimeException(ex);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    
                    // Change the current state
                    cmp.m_controlState = ControlState.PAUSED;
                    
                    // Then tell the client to drain
                    ProfileMeasurement pm = new ProfileMeasurement();
                    try {
                        if (debug.val) LOG.debug("Draining connection queue for client " + cmp.getClientId());
                        pm.start();
                        cmp.getClientHandle().drain();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    } finally {
                        pm.stop();
                        if (debug.val) LOG.debug("Drain Queue Time: " + ProfileMeasurementUtil.debug(pm));
                    }
                    break;
                }
                case SHUTDOWN: {
                    if (debug.val) LOG.debug("Shutting down client + cluster");
                    this.stop = true;
                    if (cmp.m_controlState == ControlState.RUNNING || cmp.m_controlState == ControlState.PAUSED) {
                        cmp.invokeStopCallback();
                        try {
                            client.drain();
                            client.callProcedure(VoltSystemProcedure.procCallName(Shutdown.class));
                            client.close();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                    
                    break;
                }
                case STOP: {
                    this.stop = true;
                    if (cmp.m_controlState == ControlState.RUNNING || cmp.m_controlState == ControlState.PAUSED) {
                        if (debug.val) LOG.debug("Stopping client");
                        cmp.invokeStopCallback();
                        
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
                            // Ignore...
                        }
                    } else {
                        LOG.fatal("STOP when not RUNNING");
                    }
                    break;
                }
                default: {
                    throw new RuntimeException("Error on standard input: unknown command " + command);
                }
            } // SWITCH
        }
    }
    
    private String getProfileInfo() {
        return (String.format("Client #%02d - %s / %s",
                cmp.getClientId(), cmp.worker.getExecuteTime().debug(), cmp.worker.getBlockedTime().debug()));
    }

}
