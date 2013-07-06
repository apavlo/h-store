package edu.brown.api;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.utils.Pair;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObservableExceptionHandler;
import edu.brown.utils.EventObserver;
import edu.brown.utils.ThreadUtil;

public class BenchmarkComponentSet implements Runnable {
    private static final Logger LOG = Logger.getLogger(BenchmarkComponentSet.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    // ----------------------------------------------------------------------------
    // INTERNAL DATA MEMBERS
    // ----------------------------------------------------------------------------


    private final int clientIds[];
    private final Map<BenchmarkComponent, PrintWriter> components = new HashMap<BenchmarkComponent, PrintWriter>();
    private final Map<BenchmarkComponent, Thread> threads = new HashMap<BenchmarkComponent, Thread>();
    
    private final EventObservableExceptionHandler exceptionHandler = new EventObservableExceptionHandler();
    private final EventObserver<Pair<Thread, Throwable>> exceptionObserver = new EventObserver<Pair<Thread,Throwable>>() {
        @Override
        public void update(EventObservable<Pair<Thread, Throwable>> o, Pair<Thread, Throwable> arg) {
            arg.getSecond().printStackTrace();
        }
    };
    
    /**
     * Constructor
     * @param componentClass
     * @param clientIds
     * @param args
     * @throws Exception
     */
    public BenchmarkComponentSet(final Class<? extends BenchmarkComponent> componentClass, final int clientIds[], final String args[]) throws Exception {
        Thread.setDefaultUncaughtExceptionHandler(this.exceptionHandler);
        Thread.currentThread().setUncaughtExceptionHandler(this.exceptionHandler);
        this.exceptionHandler.addObserver(this.exceptionObserver);
        this.clientIds = clientIds;
        
        final List<Runnable> runnables = new ArrayList<Runnable>();
        for (int i = 0; i < this.clientIds.length; i++) {
            final int clientId = this.clientIds[i];
            final PipedInputStream in = new PipedInputStream();
            final PipedOutputStream out = new PipedOutputStream(in);
            final PrintWriter writer = new PrintWriter(out);
            
            runnables.add(new Runnable() {
                public void run() {
                    Collection<String> clientArgs = CollectionUtil.addAll(new ArrayList<String>(), args);
                    clientArgs.add("ID=" + clientId);
                    BenchmarkComponent comp = BenchmarkComponent.main(componentClass,
                                                                      clientArgs.toArray(new String[0]),
                                                                      false);
                    synchronized (BenchmarkComponentSet.this.components) {
                        BenchmarkComponentSet.this.components.put(comp, writer);
                    } // SYNCH

                    if (debug.val) LOG.debug("Starting Client thread " + clientId);
                    Thread t = new Thread(comp.createControlPipe(in));
                    t.setDaemon(true);
                    t.start();
                    synchronized (BenchmarkComponentSet.this.threads) {
                        BenchmarkComponentSet.this.threads.put(comp, t);
                    } // SYNCH
                }
            });
        } // FOR
        ThreadUtil.runGlobalPool(runnables);
        assert(runnables.size() == this.threads.size());
        assert(runnables.size() == this.components.size());
    }
    
    @Override
    public void run() {
        String line = null;
        final BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            if (debug.val) LOG.debug("Blocking on input stream for commands from BenchmarkController");
            try {
                line = in.readLine().trim();
            } catch (final IOException e) {
                throw new RuntimeException("Error on standard input", e);
            }

            if (debug.val) LOG.debug("New Command: " + line);
            for (PrintWriter out : this.components.values()) {
                out.println(line);
                out.flush();
            } // FOR
        } // WHILE
    }
    
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        // First argument is the class we are going to need to invoke
        // Everything else is the options that we need to pass to it
        String clientClass = args[0];
        assert(clientClass.isEmpty() == false);

        Class<? extends BenchmarkComponent> componentClass = (Class<? extends BenchmarkComponent>)ClassUtil.getClass(clientClass);
        assert(componentClass != null);

        // Get the list of ClientIds that we need to start
        int clientIds[] = null;
        String componentArgs[] = new String[args.length - 2];
        int compIdx = 0;
        for (int i = 1; i < args.length; i++) {
            final String arg = args[i];
            final String[] parts = arg.split("=", 2);
            assert(parts.length >= 1) : "Invalid parameter: " + arg;

            if (parts[0].equalsIgnoreCase("ID")) {
                String ids[] = parts[1].split(",");
                clientIds = new int[ids.length];
                for (int j = 0; j < ids.length; j++) {
                    clientIds[j] = Integer.parseInt(ids[j]);
                } // FOR
            } else {
                componentArgs[compIdx++] = args[i];
            }
        } // FOR
        
        if (debug.val)
            LOG.info(String.format("Starting %d BenchmarkComponent threads: %s",
                     clientIds.length, Arrays.toString(clientIds)));
        BenchmarkComponentSet bcs = new BenchmarkComponentSet(componentClass, clientIds, componentArgs);
        bcs.run(); // BLOCK!
    }
}
