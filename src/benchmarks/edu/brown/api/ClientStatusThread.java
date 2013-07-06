package edu.brown.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.catalog.Database;
import org.voltdb.processtools.ProcessSetManager;

import edu.brown.api.results.BenchmarkComponentResults;
import edu.brown.api.results.BenchmarkResults;
import edu.brown.api.results.ResponseEntries;
import edu.brown.catalog.CatalogUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.StringUtil;

/**
 * This thread is started by the BenchmarkController and will process
 * updates coming from remotely started BenchmarkComponents
 * @author pavlo
 */
public class ClientStatusThread extends Thread {
    private static final Logger LOG = Logger.getLogger(ClientStatusThread.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }

//    private final int thread_id;
    
    /** ClientName -> List of all the Previous Messages */
    private final Map<String, List<ProcessSetManager.OutputLine>> previous = new HashMap<String, List<ProcessSetManager.OutputLine>>();
    
    /** ClientName -> Timestamp of Previous Message */ 
    private final Map<String, Long> lastTimestamps = new HashMap<String, Long>();
    
    /** TransactionName -> # of Executed **/
    private final Map<String, Long> results = new HashMap<String, Long>();
    
    private boolean finished = false;
    
    private final BenchmarkController controller;
    private final BenchmarkResults m_currentResults;
    
    private final BenchmarkComponentResults tc = new BenchmarkComponentResults();
    private final ResponseEntries responseEntries = new ResponseEntries();
    private CountDownLatch responseEntriesLatch;
    
    public ClientStatusThread(BenchmarkController controller, int i) {
        super(String.format("client-status-%02d", i));
        this.controller = controller;
        this.m_currentResults = controller.getBenchmarkResults();
        this.setDaemon(true);
    }
    
    public boolean isFinished() {
        return (this.finished);
    }
    
    @Override
    public void run() {
        this.finished = false;
        final Database catalog_db = CatalogUtil.getDatabase(controller.getCatalog());
        final CountDownLatch resultsToRead = controller.getResultsToReadLatch();
        final ProcessSetManager clientPSM = controller.getClientProcessSetManager();

        while (resultsToRead.getCount() > 0) {
            ProcessSetManager.OutputLine line = clientPSM.nextBlocking();
            if (line == null) {
                continue;
            }
            // Print stderr back-out to the console
            else if (line.stream == ProcessSetManager.StreamType.STDERR) {
                String prefix = String.format("(%s): ", line.processName);
                System.err.println(StringUtil.prefix(line.value, prefix));
                continue;
            }
            // General Debug Output
            else if (line.value.startsWith(BenchmarkComponent.CONTROL_MESSAGE_PREFIX) == false) {
                String prefix = String.format("(%s): ", line.processName);
                System.out.println(StringUtil.prefix(line.value, prefix));
                continue;
            }
            
            // BenchmarkController Coordination Message
            // split the string on commas and strip whitespace
            String control_line = line.value.substring(BenchmarkComponent.CONTROL_MESSAGE_PREFIX.length());
            String[] parts = control_line.split(",");
            for (int i = 0; i < parts.length; i++)
                parts[i] = parts[i].trim();

            // expect at least time and status
            if (parts.length < 2) {
                if (line.value.startsWith("Listening for transport dt_socket at address:") ||
                        line.value.contains("Attempting to load") ||
                        line.value.contains("Successfully loaded native VoltDB library")) {
                    LOG.info(line.processName + ": " + control_line + "\n");
                    continue;
                }
//                m_clientPSM.killProcess(line.processName);
//                LogKeys logkey =
//                    LogKeys.benchmark_BenchmarkController_ProcessReturnedMalformedLine;
//                LOG.l7dlog( Level.ERROR, logkey.name(),
//                        new Object[] { line.processName, line.value }, null);
                continue;
            }

            int clientId = -1;
            long time = -1;
            try {
                clientId = Integer.parseInt(parts[0]);
                time = Long.parseLong(parts[1]);
            } catch (NumberFormatException ex) {
                LOG.warn("Failed to parse line '" + control_line + "'", ex);
                continue; // IGNORE
            }
            final String clientName = BenchmarkControllerUtil.getClientName(line.processName, clientId);
            final ControlState status = ControlState.get(parts[2]);
            assert(status != null) : "Unexpected ControlStatus '" + parts[2] + "'";
            
            if (debug.val) 
                LOG.debug(String.format("Client %s -> %s", clientName, status));
            
            // Make sure that we never go back in time!
            Long lastTimestamp = this.lastTimestamps.get(clientName);
            if (lastTimestamp != null) assert(time >= lastTimestamp) :
                String.format("New message from %s is in the past [newTime=%d, lastTime=%d]", clientName, time, lastTimestamp);

            switch (status) {
                // ----------------------------------------------------------------------------
                // READY
                // ----------------------------------------------------------------------------
                case READY: {
                    if (debug.val) LOG.debug(String.format("Got ready message for '%s'.", line.processName));
                    controller.clientIsReady(clientName);
                    break;
                }
                // ----------------------------------------------------------------------------
                // ERROR
                // ----------------------------------------------------------------------------
                case ERROR: {
                    clientPSM.killProcess(line.processName);
                    LOG.error(String.format("(%s) Returned error message:\n\"%s\"", line.processName, parts[2]));
                    break;
                }
                // ----------------------------------------------------------------------------
                // DUMPING
                // ----------------------------------------------------------------------------
                case DUMPING: {
                    ResponseEntries newEntries = new ResponseEntries();
                    String json_line = getPayload(control_line, parts);
                    JSONObject json_object;
                    if (debug.val) LOG.debug("Processing response dump");
                    try {
                        json_object = new JSONObject(json_line);
                        newEntries.fromJSON(json_object, catalog_db);
                    } catch (JSONException ex) {
                        LOG.error("Invalid response:\n" + json_line);
                        throw new RuntimeException(ex);
                    }
                    this.responseEntries.addAll(newEntries);
                    if (this.responseEntriesLatch != null) this.responseEntriesLatch.countDown();
                    resultsToRead.countDown();
                    break;
                }
                // ----------------------------------------------------------------------------
                // RUNNING
                // ----------------------------------------------------------------------------
                case RUNNING: {
                    // System.out.println("Got running message: " + Arrays.toString(parts));
                    if (parts[parts.length-1].equalsIgnoreCase("OK")) continue;
                    
                    this.tc.clear(true);
                    String json_line = getPayload(control_line, parts);
                    JSONObject json_object;
                    try {
                        json_object = new JSONObject(json_line);
                        this.tc.fromJSON(json_object, catalog_db);
                    } catch (JSONException ex) {
                        LOG.error("Invalid response:\n" + json_line);
                        throw new RuntimeException(ex);
                    }
                    assert(json_object != null);
                    if (debug.val) LOG.debug("Base Partitions:\n " + this.tc.basePartitions); 
                    
//                    this.results.clear();
//                    for (String txnName : tc.transactions.values()) {
//                        this.results.put(txnName, tc.transactions.get(txnName));
//                    } // FOR
                    
                    try {
                        if (debug.val) LOG.debug("UPDATE: " + line);
                        this.addPollResponseInfo(clientName, time, this.tc, null);
                    } catch (Throwable ex) {
                        List<ProcessSetManager.OutputLine> p = this.previous.get(clientName);
                        LOG.error(String.format("Invalid response from '%s':\n%s\n%s\n", clientName, JSONUtil.format(json_object), line, results), ex);
                        LOG.error(String.format("Previous Lines for %s [%s]:\n%s",
                                                clientName,
                                                (p != null ? p.size() : p),
                                                StringUtil.join("\n", p)));
                        throw new RuntimeException(ex);
                    }
                    List<ProcessSetManager.OutputLine> p = this.previous.get(clientName);
                    if (p == null) {
                        p = new ArrayList<ProcessSetManager.OutputLine>();
                        this.previous.put(clientName, p);
                    }
                    p.add(line);
                    resultsToRead.countDown();
                    break;
                }
                default:
                    assert(false) : "Unexpected ControlStatus " + status;
            } // SWITCH
            
            this.lastTimestamps.put(clientName, time);
        } // WHILE
        if (debug.val)
            LOG.debug("Status thread is finished");
        this.finished = true;
    }
    
    public ResponseEntries getResponseEntries() {
        return (this.responseEntries);
    }
    
    public void addResponseEntriesLatch(CountDownLatch latch) {
        this.responseEntriesLatch = latch;
    }
    
    private String getPayload(String control_line, String parts[]) {
        int offset = 1;
        for (int i = 0; i < 3; i++) {
            offset += parts[i].length() + 1;
        } // FOR
        return (control_line.substring(offset));
    }
    
    private void addPollResponseInfo(String clientName, long time, BenchmarkComponentResults tc, String errMsg) {
        assert(m_currentResults != null);
        
        // Update Transaction Counters
        BenchmarkResults resultCopy = m_currentResults.addPollResponseInfo(
                clientName,
                controller.m_pollIndex - 1,
                time,
                tc,
                errMsg);
        if (resultCopy != null) {
            // notify interested parties
            for (BenchmarkInterest interest : controller.getBenchmarkInterests()) {
                interest.benchmarkHasUpdated(resultCopy);
            } // FOR
            controller.m_maxCompletedPoll = resultCopy.getCompletedIntervalCount();
        }

            // get total transactions run for this segment
//            long txnDelta = 0;
//            for (String client : resultCopy.getClientNames()) {
//                try {
//                    for (String txn : resultCopy.getTransactionNames()) {
//                        Result[] rs = resultCopy.getResultsForClientAndTransaction(client, txn);
//                        Result r = rs[rs.length - 1];
//                        txnDelta += r.transactionCount;
//                    } // FOR
//                } catch (Throwable ex) {
//                    LOG.error(StringUtil.columns(m_currentResults.toString(), resultCopy.toString()));
//                    LOG.error(client + " PREVIOUS:\n" + CollectionUtil.first(m_statusThreads).previous.get(client));
//                    throw new RuntimeException(ex);
//                }
//
//            } // FOR

            // if nothing done this segment, dump everything
//            if (txnDelta == 0) {
//                tryDumpAll();
//                System.out.println("\nDUMPING!\n");
//            }
//        }


    }
}
