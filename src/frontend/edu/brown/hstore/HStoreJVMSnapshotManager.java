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

public class HStoreJVMSnapshotManager implements Runnable {
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
	
	private BlockingQueue<LocalTransaction> queue;
	
	private ServerSocket serverSocket;
	
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
		this.channel = null;
		this.snapshot_pid = 0;
		this.isParent = true;
		this.response = null;
		
		queue = new LinkedBlockingDeque<LocalTransaction>();

		if (debug.val)
			LOG.debug(String.format("Local Partitions for Site #%d: %s",
					hstore_site.getSiteId(), hstore_site.getLocalPartitionIds()));

		// Incoming RPC Handler
        serverSocket = null;
        Integer local_port = null;
        try {
			local_port = this.catalog_site.getJVMSnapshot_port();
			serverSocket = new ServerSocket(local_port);
        } catch (IOException e) {
            LOG.info("Could not listen on port: "+local_port);
        }
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
			/*
			InetSocketAddress destinationAddress = new InetSocketAddress(
					this.catalog_site.getHost().getIpaddr(),
					this.catalog_site.getJVMSnapshot_port());
			if (debug.val)
				LOG.debug("Connecting to child address "
						+ destinationAddress.getHostString() + " "
						+ destinationAddress.getPort());

			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			ProtoRpcChannel[] channels = null;
			int tries = 3;
            boolean success = false;
			this.eventLoop = new NIOEventLoop();
            while (tries-- > 0 && success == false) {
                try {
                    channels = ProtoRpcChannel.connectParallel(eventLoop,
                                                               new InetSocketAddress[] { destinationAddress },
                                                               hstore_site.getHStoreConf().site.network_startup_wait);
                    success = true;
                } catch (Throwable ex) {
                    if (tries > 0) {
                        LOG.warn("Failed to connect to snapshot. Going to try again...");
                        continue;
                    }
                }
            } // WHILE
            if (success == false) {
                LOG.info("Site #" + this.getLocalSiteId() + " failed to connect to snapshot");
                return false;
            }

			this.channel = HStoreJVMSnapshotService.newStub(channels[0]);
			//listener_thread = new Thread(new ListenerThread());
			//listener_thread.start();

			if (debug.val)
				LOG.debug("Site #" + this.getLocalSiteId()
						+ " is connected to the new JVM snapshot");
			*/
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
				e.printStackTrace();
			}
			
            Socket kkSocket = null;
     
            try {
            	int local_port = this.catalog_site.getJVMSnapshot_port();
                kkSocket = new Socket("localhost", local_port);
                out = new DataOutputStream(kkSocket.getOutputStream());
                in = new DataInputStream(kkSocket.getInputStream());
                while (true) {
	                int len = in.readInt();
	                if (len == 0) break;
		            byte[] barr = new byte[len];
		            in.readFully(barr);
		            FastDeserializer des = new FastDeserializer(barr);
		            LocalTransaction ts = new LocalTransaction(hstore_site);
		    		try {
		    			ts.readExternal(des);
		    		} catch (IOException e) {
		    			// TODO Auto-generated catch block
		    			e.printStackTrace();
		    		}
		    		hstore_site.transactionQueue(ts);
                }
                in.close();
                out.close();
                kkSocket.close();
                
            } catch (UnknownHostException e) {
                System.exit(1);
            } catch (IOException e) {
                System.exit(1);
            }
			
			/*			

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
			try {
				eventLoop.run();
			} catch (Throwable ex) {
				if (debug.val)
					LOG.debug("SnapshotsListener error", ex);
			}
			if (debug.val)
				LOG.debug("Never reach here");
			System.exit(-1);
			*/
			return false;
		}

	}
	
	public void addTransactionRequest(LocalTransaction ts) {
		try {
			this.queue.put(ts);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
			hstore_site.getTransactionInitializer().registerOldTransaction(ts);
			if (debug.val)
				LOG.debug("Snapshot receive a LocalTransaction Object: "
						+ ts.toStringImpl());
			hstore_site.transactionQueue(ts);

			int aa = 0;
			while (response == null) {
				aa++;
				if (aa % 1000 == 0 && debug.val)
					LOG.debug("sleep" + (aa/1000));
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
			e.printStackTrace();
		}
		//this.response = TransactionResponse.newBuilder().setOutput(bs).build();
		try {
			out.writeInt(bs.size());
			out.write(bs.toByteArray());
			out.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		ByteString bs = ByteString.EMPTY;
		TransactionRequest tr = TransactionRequest.newBuilder().setRequest(bs)
				.build();
		JVMSnapshotTransactionCallback callback = new JVMSnapshotTransactionCallback(
				hstore_site, null);
		channel.execTransactionRequest(new ProtoRpcController(), tr, callback);
		if (debug.val)
			LOG.debug("Send finish;");
        /*
		try {
			Runtime.getRuntime().exec("kill -9 "+this.snapshot_pid);
		} catch (IOException e) {
			e.printStackTrace();
		}
        */
		//ProcessUtils.kill(this.snapshot_pid);
		if (listener_thread != null && listener_thread.isAlive()) {
			eventLoop.exitLoop();		
		}
        

		this.snapshot_pid = 0;
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
		while (true) {
			LocalTransaction ts = null;
			try {
				ts = this.queue.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
				continue;
			}
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
				e.printStackTrace();
			}
			/*
			TransactionRequest tr = TransactionRequest.newBuilder().setRequest(bs)
					.build();
			if (debug.val)
				LOG.debug("Send execTransactionRequest to the snapshot;");
			JVMSnapshotTransactionCallback callback = new JVMSnapshotTransactionCallback(
					hstore_site, ts);
			ProtoRpcController rpc = new ProtoRpcController();
			channel.execTransactionRequest(rpc, tr, callback);
			rpc.block();
			if (debug.val)
				LOG.debug("Send finish;");
			*/
			
            Socket clientSocket = null;
            try {
                clientSocket = serverSocket.accept();
            } catch (IOException e) {
                System.err.println("Accept failed.");
                System.exit(1);
            }
            DataOutputStream out;
			try {
				out = new DataOutputStream(clientSocket.getOutputStream());
	            DataInputStream in = new DataInputStream(
	                    clientSocket.getInputStream());
	            out.writeInt(bs.size());
	            out.write(bs.toByteArray());
	            out.flush();
	            int len = in.readInt();
	            byte[] barr = new byte[len];
	            in.readFully(barr);
	            FastDeserializer des = new FastDeserializer(barr);
	    		ClientResponseImpl response = new ClientResponseImpl();
	    		try {
	    			response.readExternal(des);
	    		} catch (IOException e) {
	    			// TODO Auto-generated catch block
	    			e.printStackTrace();
	    		}
	    		if (debug.val) LOG.debug("Msg: "+response.toString());
	    		
	    		this.hstore_site.responseSend(ts, response);
	            
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
           
            
			
			
		}
		
	}
	
	public void notifyFinish() {
		synchronized (this) {
			this.notify();
		}
	}

}
