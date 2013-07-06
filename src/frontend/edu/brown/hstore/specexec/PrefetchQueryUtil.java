package edu.brown.hstore.specexec;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.ParameterSet;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.messaging.FastDeserializer;

import com.google.protobuf.ByteString;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Utility methods for processing prefetch queries in txns
 * @author pavlo
 */
public abstract class PrefetchQueryUtil {
    private static final Logger LOG = Logger.getLogger(PrefetchQueryUtil.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }

    /**
     * 
     * @param hstore_site
     * @param ts
     * @param fd
     * @param partition
     * @return
     */
    public static boolean dispatchPrefetchQueries(final HStoreSite hstore_site,
                                                  final AbstractTransaction ts,
                                                  final FastDeserializer fd,
                                                  final int partition) {
        if (debug.val)
            LOG.debug(String.format("%s - Looking to dispatch prefetch %s for partition %d",
                      ts, WorkFragment.class.getSimpleName(), partition));
        
        // Go through the prefetch WorkFragments and find the one that we need to send
        // to our target partition. We'll then queue it up at its corresponding PartitionExecutor
        boolean sent = false;
        for (WorkFragment frag : ts.getPrefetchFragments()) {
            if (frag.getPartitionId() == partition) {
                if (debug.val)
                    LOG.debug(String.format("%s - Dispatching prefetch %s to partition %d",
                              ts, frag.getClass().getSimpleName(), partition));
                // We need to convert our raw ByteString ParameterSets into the actual objects
                if (ts.hasPrefetchParameters() == false) {
                    synchronized (ts) {
                        if (ts.hasPrefetchParameters() == false) {
                            convertPrefetchParameters(ts, fd);
                        }
                    } // SYNCH
                }
                hstore_site.transactionWork(ts, frag);
                sent = true;
                break;
            }
        } // FOR
        if (debug.val && sent == false)
            LOG.debug(String.format("%s - Did not find a prefetch %s for partition %d",
                      ts, WorkFragment.class.getSimpleName(), partition));
        return (sent);
    }
    
    protected static void convertPrefetchParameters(AbstractTransaction ts, FastDeserializer fd) {
        if (debug.val)
            LOG.debug(String.format("%s - Converting raw prefetch parameter bytes into ParameterSets", ts));
        
        List<ByteString> rawParams = ts.getPrefetchRawParameterSets(); 
        int num_parameters = rawParams.size();
        ParameterSet params[] = new ParameterSet[num_parameters]; 
        for (int i = 0; i < params.length; i++) {
            fd.setBuffer(rawParams.get(i).asReadOnlyByteBuffer());
            try {
                params[i] = fd.readObject(ParameterSet.class);
            } catch (IOException ex) {
                String msg = "Failed to deserialize pre-fetch ParameterSet at offset #" + i;
                throw new ServerFaultException(msg, ex, ts.getTransactionId());
            }
            if (debug.val)
                LOG.debug(String.format("%s - Prefetch ParameterSet [%02d] -> %s",
                          ts, i, params[i]));
        } // FOR
        ts.attachPrefetchParameters(params);
    }
    
}
