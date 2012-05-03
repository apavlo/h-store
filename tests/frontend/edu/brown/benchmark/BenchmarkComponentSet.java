package edu.brown.benchmark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;


public class BenchmarkComponentSet implements Runnable {
    private static final Logger LOG = Logger.getLogger(BenchmarkComponentSet.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
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
    
    
    public BenchmarkComponentSet(Class<? extends BenchmarkComponent> componentClass, int clientIds[], String args[]) throws Exception {
        this.clientIds = clientIds;
        List<String> clientArgs = (List<String>)CollectionUtil.addAll(new ArrayList<String>(), args);
        for (int i = 0; i < this.clientIds.length; i++) {
            clientArgs.add("ID=" + this.clientIds[i]);
            BenchmarkComponent comp = BenchmarkComponent.main(componentClass, clientArgs.toArray(new String[0]), false);
            PipedInputStream in = new PipedInputStream();
            PipedOutputStream out = new PipedOutputStream(in);
            this.components.put(comp, new PrintWriter(out));

            if (debug.get()) LOG.debug("Starting Client thread " + this.clientIds[i]);
            Thread t = new Thread(comp.createControlPipe(in));
            t.setDaemon(true);
            t.start();
            this.threads.put(comp, t);
            
            clientArgs.remove(clientArgs.size()-1);
        } // FOR
    }
    
    public void run() {
        String line = null;
        final BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            if (debug.get()) LOG.debug("Blocking on input stream for commands from BenchmarkController");
            try {
                line = in.readLine();
            } catch (final IOException e) {
                throw new RuntimeException("Error on standard input", e);
            }
            
            if (debug.get()) LOG.debug("New Command: " + line);
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
        
        if (debug.get()) LOG.info(String.format("Starting %d BenchmarkComponent threads: %s",
                                                clientIds.length, Arrays.toString(clientIds)));
        BenchmarkComponentSet bcs = new BenchmarkComponentSet(componentClass, clientIds, componentArgs);
        bcs.run(); // BLOCK!
    }
}
