package edu.brown.hstore;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.catalog.Site;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.ProcessUtils;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Jvmsnapshot.HStoreJVMSnapshotService;
import edu.brown.hstore.Jvmsnapshot.TransactionRequest;
import edu.brown.hstore.Jvmsnapshot.TransactionResponse;
import edu.brown.hstore.callbacks.JVMSnapshotTransactionCallback;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.protorpc.NIOEventLoop;
import edu.brown.protorpc.ProtoRpcChannel;
import edu.brown.protorpc.ProtoRpcController;
import edu.brown.protorpc.ProtoServer;

public class HStoreJVMSnapshotManager {
	private static final Logger LOG = Logger
			.getLogger(HStoreJVMSnapshotManager.class);
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

	private NIOEventLoop eventLoop = new NIOEventLoop();

	// for parent
	private HStoreJVMSnapshotService channel;
	private Thread listener_thread;

	// for child snapshots
	private ProtoServer listener;
	private SnapshotHandler snapshotHandler;

	private int snapshot_pid;
	private boolean isParent;

	private TransactionResponse response;

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
		this.channel = null;
		this.snapshot_pid = 0;
		this.isParent = true;
		this.response = null;

		if (debug.val)
			LOG.debug(String.format("Local Partitions for Site #%d: %s",
					hstore_site.getSiteId(), hstore_site.getLocalPartitionIds()));

		// Incoming RPC Handler
	}

	protected int getLocalSiteId() {
		return (this.local_site_id);
	}

	protected int getJVMSnapshotPort() {
		return (this.hstore_site.getSite().getJVMSnapshot_port());
	}

	public HStoreJVMSnapshotService getChannel() {
		return (this.channel);
	}

	public HStoreJVMSnapshotService getHandler() {
		return (this.snapshotHandler);
	}

	public boolean isParent() {
		return isParent;
	}

	// ----------------------------------------------------------------------------
	// LISTENER THREAD
	// ----------------------------------------------------------------------------

	private class ListenerThread implements Runnable {
		@Override
		public void run() {
			try {
				if (debug.val)
					LOG.debug("Parent start listening");
				eventLoop.run();
			} catch (Throwable ex) {
				if (debug.val)
					LOG.debug("ListenerThread error", ex);
			}
			if (debug.val)
				LOG.debug("Never reach here");

		}
	}

	/**
	 * Fork a new snapshot. This is a blocking call that will initialize the
	 * snapshot and set up the connection!
	 * 
	 */
	private boolean forkNewSnapShot() {

		if (debug.val)
			LOG.debug("Fork new JVM snapshot for Site #"
					+ this.catalog_site.getId());

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
			// Connect to the child snapshot.
			InetSocketAddress destinationAddress = new InetSocketAddress(
					this.catalog_site.getHost().getIpaddr(),
					this.catalog_site.getJVMSnapshot_port());
			if (debug.val)
				LOG.debug("Connecting to child address "
						+ destinationAddress.getHostString() + " "
						+ destinationAddress.getPort());

			ProtoRpcChannel[] channels = null;
			try {
				channels = ProtoRpcChannel.connectParallel(
					eventLoop, new InetSocketAddress[] { destinationAddress });
			} catch (Exception e) {
				LOG.info("Connection fail", e);
				return false;
			}

			this.channel = HStoreJVMSnapshotService.newStub(channels[0]);
			listener_thread = new Thread(new ListenerThread());
			listener_thread.start();

			if (debug.val)
				LOG.debug("Site #" + this.getLocalSiteId()
						+ " is connected to the new JVM snapshot");
			return true;
		} else {
			// child process
			Thread self = Thread.currentThread();
			self.setName(HStoreThreadManager
					.getThreadName(hstore_site, "child"));
			this.isParent = false;
			if (debug.val)
				LOG.debug("Child process start");
			hstore_site.getHStoreConf().site.txn_counters = false;

			this.hstore_site.snapshot_init();
			
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// Initialize listener
			this.eventLoop = new NIOEventLoop();
			this.listener = new ProtoServer(this.eventLoop);
			this.snapshotHandler = new SnapshotHandler();
			Integer local_port = this.catalog_site.getJVMSnapshot_port();
			assert (local_port != null);
			if (debug.val)
				LOG.debug("Binding listener to port " + local_port
						+ " for Site #" + this.catalog_site.getId());
			this.listener.register(this.snapshotHandler);
			this.listener.bind(local_port);

			this.eventLoop.setExitOnSigInt(true);

			if (debug.val)
				LOG.debug("New Snapshot start to listen on port");
			System.out.flush();
			try {
				eventLoop.run();
			} catch (Throwable ex) {
				if (debug.val)
					LOG.debug("SnapshotsListener error", ex);
			}
			if (debug.val)
				LOG.debug("Never reach here");
			System.exit(-1);
			return false;
		}

	}

	// ----------------------------------------------------------------------------
	// HSTORE RPC SERVICE METHODS
	// ----------------------------------------------------------------------------

	public void execTransactionRequest(LocalTransaction ts) {
		if (debug.val)
			LOG.debug("Send execTransactionRequest to the snapshot;");
		if (!isParent)
			return;
		if (snapshot_pid == 0 || refresh == true) {
			stopSnapshot();
			if (!forkNewSnapShot()) {
				stopSnapshot();
				hstore_site.responseError(
						ts.getClientHandle(), 
						Status.ABORT_CONNECTION_LOST, 
						"Forking Snapshot fails",
						ts.getClientCallback(),
						ts.getInitiateTime());
				return;
			};
			refresh = false;
		}

		ByteString bs = ByteString.EMPTY;
		try {
			bs = ByteString.copyFrom(FastSerializer.serialize(ts));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		TransactionRequest tr = TransactionRequest.newBuilder().setRequest(bs)
				.build();
		if (debug.val)
			LOG.debug("Send execTransactionRequest to the snapshot;");
		JVMSnapshotTransactionCallback callback = new JVMSnapshotTransactionCallback(
				ts.getClientHandle(), ts.getClientCallback());
		channel.execTransactionRequest(new ProtoRpcController(), tr, callback);
		if (debug.val)
			LOG.debug("Send finish;");

	}

	// ----------------------------------------------------------------------------
	// HSTORE SNAPSHOT RPC SERVICE METHODS
	// ----------------------------------------------------------------------------

	/**
	 * We want to make this a private inner class so that we do not expose the
	 * RPC methods to other parts of the code.
	 */
	private class SnapshotHandler extends HStoreJVMSnapshotService {

		@Override
		public void execTransactionRequest(RpcController controller,
				TransactionRequest request,
				RpcCallback<TransactionResponse> done) {
			if (debug.val)
				LOG.debug("Snapshot receives a execTransactionRequest from the parent!");
			if (request.getRequest().isEmpty()) {
				// shut down
				if (debug.val)
					LOG.debug("Snapshot receives a shutdown from the parent!");
				System.exit(0);
			}
			FastDeserializer in = new FastDeserializer(request.getRequest()
					.toByteArray());
			LocalTransaction ts = new LocalTransaction(hstore_site);
			try {
				ts.readExternal(in);
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (debug.val)
				LOG.debug("Snapshot receive a LocalTransaction Object: "
						+ ts.toStringImpl());
			hstore_site.transactionQueue(ts);

			int aa = 0;
			while (response == null) {
				aa++;
				if (aa % 400 == 0 && debug.val)
					LOG.debug("sleep" + aa);
			}
			if (debug.val) {
				LOG.debug("Snapshot send back response to the parent!");
			}
			done.run(response);
			response = null;
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.response = TransactionResponse.newBuilder().setOutput(bs).build();
		if (debug.val)
			LOG.debug("Generate response "
					+ this.response.toString());

	}

	// ----------------------------------------------------------------------------
	// SHUTDOWN (Called By Parent)
	// ----------------------------------------------------------------------------

	public void stopSnapshot() {
		if (debug.val)
			LOG.debug("HStoreJVMSnapshot shutdown!");
		if (!isParent || snapshot_pid == 0)
			return;
		ProcessUtils.kill(this.snapshot_pid);
		if (listener_thread != null && listener_thread.isAlive()) {
			eventLoop.exitLoop();		
		}
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		this.snapshot_pid = 0;
	}

	public void refresh() {
		this.refresh = true;
		
	}

}
