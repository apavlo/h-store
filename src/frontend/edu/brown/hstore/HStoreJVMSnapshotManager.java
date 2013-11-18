package edu.brown.hstore;

import java.io.*;
import java.net.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.catalog.Site;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.ProcessUtils;

import com.google.protobuf.ByteString;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class HStoreJVMSnapshotManager implements Runnable {
    private static final Logger LOG = Logger.getLogger(HStoreJVMSnapshotManager.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    // ----------------------------------------------------------------------------
    // INTERNAL STATE
    // ----------------------------------------------------------------------------

    private final HStoreSite hstore_site;
    private final Site catalog_site;
    private final int local_site_id;

    private boolean refresh;

    private int snapshot_pid;
    private boolean isParent;

    private BlockingQueue<LocalTransaction> queue;

    private ServerSocket serverSocket;
    private Socket clientSocket;

    private DataOutputStream out;
    private DataInputStream in;

    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------

    /**
     * Constructor
     * 
     * @param hstore_site
     */
    public HStoreJVMSnapshotManager(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.catalog_site = this.hstore_site.getSite();
        this.local_site_id = this.catalog_site.getId();
        this.snapshot_pid = 0;
        this.isParent = true;
        queue = new LinkedBlockingDeque<LocalTransaction>();

        if (debug.val)
            LOG.debug(String.format("Local Partitions for Site #%d: %s", hstore_site.getSiteId(), hstore_site.getLocalPartitionIds()));

        // Incoming RPC Handler
        serverSocket = null;
        Integer local_port = null;
        try {
            local_port = this.catalog_site.getJVMSnapshot_port();
            serverSocket = new ServerSocket(local_port);
            serverSocket.setSoTimeout(5000);
        } catch (IOException e) {
            LOG.info("Could not listen on port: " + local_port);
        }
    }

    protected int getLocalSiteId() {
        return (this.local_site_id);
    }

    protected int getJVMSnapshotPort() {
        return (this.hstore_site.getSite().getJVMSnapshot_port());
    }

    public boolean isParent() {
        return isParent;
    }

    /**
     * Fork a new snapshot. This is a blocking call that will initialize the
     * snapshot and set up the connection!
     */
    private boolean forkNewSnapShot() {

        if (debug.val)
            LOG.debug("Fork new JVM snapshot for Site #" + this.catalog_site.getId());

        int pid = ProcessUtils.fork();
        if (pid == -1) {
            if (debug.val)
                LOG.debug("Fork new JVM snapshot fails.");
            return false;
        }
        if (pid != 0) {
            // parent process
            if (debug.val)
                LOG.debug("Fork Child process " + pid);
            snapshot_pid = pid;
            return true;
        } else {
            Thread self = Thread.currentThread();
            self.setName(HStoreThreadManager.getThreadName(hstore_site, "child"));
            this.isParent = false;
            if (debug.val)
                LOG.debug("Child process start");
            hstore_site.getHStoreConf().site.txn_counters = false;
            runSnapshot();
        }
        

        return false;

    }
    
    private void runSnapshot() {
        // child process

        this.hstore_site.snapshot_init();

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Socket kkSocket = null;

        try {
            int local_port = this.catalog_site.getJVMSnapshot_port();
            kkSocket = new Socket(this.catalog_site.getHost().getIpaddr(), local_port);
            out = new DataOutputStream(kkSocket.getOutputStream());
            in = new DataInputStream(kkSocket.getInputStream());
            while (true) {
                int len = in.readInt();
                if (len == 0) { // Shutdown request
                    if (debug.val)
                        LOG.debug("Get shutdown message from parent");
                    break;
                }
                byte[] barr = new byte[len];
                in.readFully(barr);
                FastDeserializer des = new FastDeserializer(barr);
                LocalTransaction ts = new LocalTransaction(hstore_site);
                ts.readExternal(des);
                if (debug.val)
                    LOG.debug("Get a new transaction from parent");
                hstore_site.getTransactionInitializer().registerOldTransaction(ts);
                hstore_site.transactionQueue(ts);
            }
        } catch (Exception e) {
            LOG.error("Snapshot error", e);
        } finally {
            try {
                kkSocket.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            System.exit(0);
        }
        
    }

    public void addTransactionRequest(LocalTransaction ts) {
        try {
            this.queue.put(ts);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // ----------------------------------------------------------------------------
    // HSTORE SNAPSHOT RESPONSE METHOD
    // ----------------------------------------------------------------------------

    public void sendResponseToParent(ClientResponseImpl response) {
        ByteString bs = ByteString.EMPTY;
        try {
            bs = ByteString.copyFrom(FastSerializer.serialize(response));
        } catch (IOException e) {
            e.printStackTrace();
        }
        // this.response =
        // TransactionResponse.newBuilder().setOutput(bs).build();
        try {
            out.writeInt(bs.size());
            out.write(bs.toByteArray());
            out.flush();
            if (debug.val)
                LOG.debug("Send back response");
        } catch (IOException e) {
            System.exit(1);
        }

    }

    // ----------------------------------------------------------------------------
    // SHUTDOWN (Called By Parent)
    // ----------------------------------------------------------------------------

    public void stopSnapshot() {

        if (!isParent || snapshot_pid == 0 || out == null)
            return;
        if (debug.val)
            LOG.debug("HStoreJVMSnapshot shutdown!");
        try {
            out.writeInt(0);
            out.flush();
            in.readInt();
        } catch (IOException e) {
        }
        if (debug.val)
            LOG.debug("Shut down successfully");
        this.snapshot_pid = 0;
        this.out = null;
        this.in = null;
    }

    public void refresh() {
        this.refresh = true;

    }

    @Override
    public void run() {
        hstore_site.getThreadManager().registerProcessingThread();
        if (hstore_site.getHStoreConf().site.jvmsnapshot_start == true) {
            this.forkNewSnapShot();
            this.refresh = false;
        }
        LocalTransaction ts = null;
        out = null;
        in = null;
        while (true) {
            try {
                ts = this.queue.take();
            } catch (InterruptedException e) {
                LOG.error("JVM Snapshot Mananger blocking queue error", e);
                continue;
            }
            // Fork if necessary
            if (snapshot_pid == 0 || refresh == true) {
                stopSnapshot();
                if (!forkNewSnapShot()) {
                    hstore_site.responseError(ts.getClientHandle(), Status.ABORT_CONNECTION_LOST, "Forking Snapshot fails", ts.getClientCallback(), ts.getInitiateTime());
                    return;
                }
                refresh = false;
            }
            try {
                clientSocket = serverSocket.accept();
                out = new DataOutputStream(clientSocket.getOutputStream());
                in = new DataInputStream(clientSocket.getInputStream());
                // write request
                ByteString bs = ByteString.EMPTY;
                bs = ByteString.copyFrom(FastSerializer.serialize(ts));
                out.writeInt(bs.size());
                out.write(bs.toByteArray());
                out.flush();
                if (debug.val)
                    LOG.debug("Send out request to the snapshot");
                // read response
                int len = in.readInt();
                byte[] barr = new byte[len];
                in.readFully(barr);
                FastDeserializer des = new FastDeserializer(barr);
                ClientResponseImpl response = new ClientResponseImpl();
                response.readExternal(des);
                if (debug.val)
                    LOG.debug("Msg: " + response.toString());

                this.hstore_site.responseSend(ts, response);

            } catch (IOException e) {
                LOG.error("", e);
                this.hstore_site.responseError(
                        ts.getClientHandle(), 
                        Status.ABORT_CONNECTION_LOST,
                        "Fail to execute on snapshot",
                        ts.getClientCallback(), 
                        ts.getInitiateTime());
                if (snapshot_pid != 0) {
                    ProcessUtils.kill(snapshot_pid);
                    snapshot_pid = 0;
                }
            }
        }

    }

}
