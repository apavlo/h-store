package edu.brown.benchmark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;
import org.voltdb.client.Client;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Implements the simple state machine for the remote controller protocol.
 * Hypothetically, you can extend this and override the answerPoll() and
 * answerStart() methods for other clients.
 */
public class ControlPipe implements Runnable {
    private static final Logger LOG = Logger.getLogger(ControlPipe.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    final InputStream in;
    final BenchmarkComponent cmp;
    
    public ControlPipe(BenchmarkComponent component, InputStream in) {
        this.cmp = component;
        this.in = in;
    }

    public void run() {
        final Thread self = Thread.currentThread();
        self.setName(String.format("client-%02d", cmp.getClientId()));

        ControlCommand command = null;
        Client client = cmp.getClientHandle();
        
        // transition to ready and send ready message
        if (cmp.m_controlState == ControlState.PREPARING) {
            cmp.printControlMessage(ControlState.READY);
            cmp.m_controlState = ControlState.READY;
        } else {
            LOG.error("Not starting prepared!");
            LOG.error(cmp.m_controlState + " " + cmp.m_reason);
        }
        
        final BufferedReader in = new BufferedReader(new InputStreamReader(this.in));
        while (true) {
            try {
                command = ControlCommand.get(in.readLine());
                if (debug.get()) 
                    LOG.debug(String.format("Recieved Message: '%s'", command));
            } catch (final IOException e) {
                // Hm. quit?
                LOG.fatal("Error on standard input", e);
                System.exit(-1);
            }
            if (command == null) continue;
            if (debug.get()) LOG.debug("ControlPipe Command = " + command);

            final Thread t = new Thread(cmp.worker);
            t.setDaemon(true);
            
            // HACK: Convert a SHUTDOWN to a STOP if we're not the first client
            if (command == ControlCommand.SHUTDOWN && cmp.getClientId() != 0) {
                command = ControlCommand.STOP;
            }
            
            switch (command) {
                case START: {
                    if (cmp.m_controlState != ControlState.READY) {
                        cmp.setState(ControlState.ERROR, "START when not READY.");
                        cmp.answerWithError();
                        continue;
                    }
                    t.start();
                    if (cmp.m_tickThread != null) cmp.m_tickThread.start();
                    cmp.m_controlState = ControlState.RUNNING;
                    cmp.answerOk();
                    break;
                }
                case POLL: {
                    if (cmp.m_controlState != ControlState.RUNNING) {
                        cmp.setState(ControlState.ERROR, "POLL when not RUNNING.");
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
                    cmp.m_txnStats.clear();
                    cmp.invokeClearCallback();
                    cmp.answerOk();
                    break;
                }
                case PAUSE: {
                    assert(cmp.m_controlState == ControlState.RUNNING) : "Unexpected " + cmp.m_controlState;
                    
                    LOG.info("Pausing client");
                    
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
