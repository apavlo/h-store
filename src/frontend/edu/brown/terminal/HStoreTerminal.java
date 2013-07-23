package edu.brown.terminal;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jline.ConsoleReader;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.NotImplementedException;
import org.voltdb.utils.VoltTableUtil;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;

/**
 * H-Store Commandline Client Terminal
 * @author gen
 * @author pavlo
 */
public class HStoreTerminal implements Runnable {
    private static final Logger LOG = Logger.getLogger(HStoreTerminal.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    
    /**
     * Special non-standard commands that we can execute
     * These are to help us mimic MySQL
     */
    public enum Command {
        DESCRIBE("Not Implemented"),
        EXEC("ProcedureName [OptionalParams]"),
        ENABLE("OptionName"),
        SHOW("Not Implemented"),
        QUIT("");
        
        private final String usage;
        private Command(String usage) {
            this.usage = usage;
        }
    };
    
    private class TerminalConnection {
        final Client client;
        final String hostname;
        final int port;
        
        public TerminalConnection(Client client, String hostname, int port) {
            this.client = client;
            this.hostname = hostname;
            this.port = port;
        }
    } // CLASS
    
    // ---------------------------------------------------------------
    // STATIC CONFIGURATION MEMBERS
    // ---------------------------------------------------------------
    
    private static final String setPlainText = StringUtil.SET_PLAIN_TEXT; 
    private static final String setBoldGreenText = "\033[1;32m"; // 0;1m";
    private static final String setBoldText = "\033[0;1m";

    private static final String PROMPT = setBoldGreenText + "hstore>" + setPlainText + " ";
    private static final Pattern SPLITTER = Pattern.compile("[ ]+");
    
    // ---------------------------------------------------------------
    // INSTANCE CONFIGURATION MEMBERS
    // ---------------------------------------------------------------
    
    private final Catalog catalog;
    private final Database catalog_db;
    private final jline.ConsoleReader reader = new ConsoleReader(); 
    private final TokenCompletor completer;
    
    // OPTIONS
    private boolean enable_csv = false; 
    private boolean enable_debug = false;
    private String hostname = null;
    private int port = HStoreConstants.DEFAULT_PORT;
    
    // ---------------------------------------------------------------
    // CONSTRUCTOR
    // ---------------------------------------------------------------
    
    public HStoreTerminal(Catalog catalog) throws Exception{
        this.catalog = catalog;
        this.catalog_db = CatalogUtil.getDatabase(this.catalog);
        
        if (debug.val) LOG.debug("Generating tab-completion keywords");
        this.completer = new TokenCompletor(catalog);
        this.reader.addCompletor(this.completer);
    }
    
    
    @Override
    public void run() {
        TerminalConnection tc = this.getClientConnection();
        if (this.enable_csv == false) {
            this.printHeader();
            System.out.printf("Connected to %s:%d / Server Version: %s\n",
                              tc.hostname, tc.port, tc.client.getBuildString());
        }
        
        String query = "";
        ClientResponse cresponse = null;
        boolean stop = false;
        try {
            do {
                try {
                    query = (this.enable_csv ? reader.readLine() : reader.readLine(PROMPT)); 
                    if (query == null || query.isEmpty()) continue;
                    query = query.trim();
                    
                    // Check if the first token is one of our special keywords
                    String tokens[] = SPLITTER.split(query);
                    
                    int retries = 3;
                    Command targetCmd = null;
                    boolean usage = false;
                    boolean reconnect = false;
                    while (retries-- > 0 && stop == false) {
                        // Check whether they want to execute a special command
                        for (Command c : Command.values()) {
                            if (tokens[0].equalsIgnoreCase(c.name())) {
                                targetCmd = c;
                            }
                        } // FOR
                        
                        try {
                            if (targetCmd != null) {
                                switch (targetCmd) {
                                    case EXEC:
                                        // The second position should be the name of the procedure
                                        // that they want to execute
                                        if (tokens.length < 2) {
                                            usage = true;
                                        } else {
                                            cresponse = this.execProcedure(tc.client, tokens[1], query, reconnect);
                                        }
                                        break;
                                    case ENABLE:
                                        this.processEnable(tc.client, tokens[1], query, reconnect);
                                        break;
                                    case QUIT:
                                        stop = true;
                                        break;
                                    case DESCRIBE:
                                    case SHOW:
                                        throw new NotImplementedException("The command '" + targetCmd + "' is is not implemented");
                                    default:
                                        throw new RuntimeException("Unexpected command '" + targetCmd);
                                } // SWITCH
                            }
                            // Otherwise we'll send it to the server to deal with as an ad-hoc query
                            else {
                                cresponse = this.execQuery(tc.client, query);
                            }
                        } catch (NoConnectionsException ex) {
                            LOG.warn("Connection lost. Going to try to connect again...");
                            tc = this.getClientConnection();
                            reconnect = true;
                            continue;
                        }
                        break;
                    } // WHILE
                    
                    // Just print out the result
                    if (cresponse != null) {
                        if (cresponse.getStatus() == Status.OK) {
                            System.out.println(this.formatResult(cresponse));    
                        } else {
                            System.out.printf("Server Response: %s / %s\n",
                                              cresponse.getStatus(),
                                              cresponse.getStatusString());
                        }
                    }
                    // Print target command usage
                    else if (usage) {
                        assert(targetCmd != null);
                        System.out.print(setBoldText);
                        System.out.println("USAGE: " + targetCmd.name() + " " + targetCmd.usage);
                        System.out.print(setPlainText);
                    }
                    // Print warning if we're not supposed to stop
                    else if (stop == false && targetCmd != Command.ENABLE) {
                        LOG.warn("Return result is null");
                    }
                    
                // Fatal Error
                } catch (RuntimeException ex) {
                    throw ex;
                // Friendly Error
                } catch (Exception ex) {
                    LOG.error(ex.getMessage());
                    Throwable cause = ex.getCause();
                    if (cause != null) {
                        LOG.error(cause.getMessage());
                        if (debug.val) cause.printStackTrace();
                    }
                }
            } while (query != null && stop == false);
        } finally {
            try {
                if (tc != null) tc.client.close();
            } catch (InterruptedException ex) {
                // Ignore
            }
        }
    }
    
    private void printHeader() {
//        System.out.print(setBoldText);
        System.out.println(" _  _     ___ _____ ___  ___ ___"); 
        System.out.println("| || |___/ __|_   _/ _ \\| _ \\ __|");
        System.out.println("| __ |___\\__ \\ | || (_) |   / _|"); 
        System.out.println("|_||_|   |___/ |_| \\___/|_|_\\___|");
        System.out.println();
//        System.out.println(setPlainText);
    }
    
    /**
     * Get a client handle to a random site in the running cluster
     * The return value includes what site the client connected to
     * @return
     */
    private TerminalConnection getClientConnection() {
        String hostname = null;
        int port = -1;
        
        // Fixed hostname
        if (this.hostname != null) {
            if (this.hostname.contains(":")) {
                String split[] = this.hostname.split("\\:", 2);
                hostname = split[0];
                port = Integer.valueOf(split[1]);
            } else {
                hostname = this.hostname;
                port = this.port;
            }
        }
        // Connect to random host and using a random port that it's listening on
        else if (this.catalog != null) {
            Site catalog_site = CollectionUtil.random(CatalogUtil.getAllSites(this.catalog));
            hostname = catalog_site.getHost().getIpaddr();
            port = catalog_site.getProc_port();
        }
        assert(hostname != null);
        assert(port > 0);
        
        if (debug.val)
            LOG.debug(String.format("Creating new client connection to %s:%d",
                      hostname, port));
        
        Client client = ClientFactory.createClient(128, null, false, null);
        try {
            client.createConnection(null, hostname, port, "user", "password");
        } catch (Exception ex) {
            String msg = String.format("Failed to connect to HStoreSite at %s:%d", hostname, port);
            throw new RuntimeException(msg);
        }
        return new TerminalConnection(client, hostname, port);
    }
    
    /**
     * Execute the given query as an ad-hoc request on the server and
     * return the result.
     * @param client
     * @param query
     * @return
     * @throws Exception
     */
    private ClientResponse execQuery(Client client, String query) throws Exception {
        if (debug.val) LOG.debug("QUERY: " + query);
        ClientResponse cresponse = client.callProcedure("@AdHoc", query);
        return (cresponse);
    }
    
    /**
     * Execute the given procedure on the server and return the result
     * @param client
     * @param procName
     * @param query
     * @return
     * @throws Exception
     */
    private ClientResponse execProcedure(Client client, String procName, String query, boolean reconnect) throws Exception {
        Procedure catalog_proc = this.catalog_db.getProcedures().getIgnoreCase(procName);
        if (catalog_proc == null) {
            throw new Exception("Invalid stored procedure name '" + procName + "'");
        }
        
        // We now need to go through the rest of the parameters and convert them
        // to proper type
        Pattern p = Pattern.compile("^" + Command.EXEC.name() + "[ ]+" + procName + "[ ]+(.*?)[;]*", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(query);
        List<Object> procParams = new ArrayList<Object>();
        if (m.matches()) {
            // Extract the parameters and then convert them to their appropriate type
            List<String> params = HStoreTerminal.extractParams(m.group(1));
            if (debug.val) LOG.debug("PARAMS: " + params);
            if (params.size() != catalog_proc.getParameters().size()) {
                String msg = String.format("Expected %d params for '%s' but %d parameters were given",
                                           catalog_proc.getParameters().size(), catalog_proc.getName(), params.size());
                throw new Exception(msg);
            }
            int i = 0;
            for (ProcParameter catalog_param : catalog_proc.getParameters()) {
                VoltType vtype = VoltType.get(catalog_param.getType());
                Object value = VoltTypeUtil.getObjectFromString(vtype, params.get(i));
                
                // HACK: Allow us to send one-element array parameters
                if (catalog_param.getIsarray()) {
                    switch (vtype) {
                        case BOOLEAN:
                            value = new boolean[]{ (Boolean)value };
                            break;
                        case TINYINT:
                        case SMALLINT:
                        case INTEGER:
                            value = new int[]{ (Integer)value };
                            break;
                        case BIGINT:
                            value = new long[]{ (Long)value };
                            break;
                        case FLOAT:
                        case DECIMAL:
                            value = new double[]{ (Double)value };
                            break;
                        case STRING:
                            value = new String[]{ (String)value };
                            break;
                        case TIMESTAMP:
                            value = new TimestampType[]{ (TimestampType)value };
                        default:
                            assert(false);
                    } // SWITCH
                }
                procParams.add(value);
                i++;
            } // FOR
        }
        
        Object params[] = procParams.toArray(); 
        if (this.enable_csv == false && reconnect == false) {
            LOG.info(String.format("Executing transaction " + setBoldText + "%s(%s)" + setPlainText, 
                     catalog_proc.getName(), StringUtil.toString(params, false, false)));
        }
        ClientResponse cresponse = client.callProcedure(catalog_proc.getName(), params);
        return (cresponse);
    }
    
    protected void processEnable(Client client, String option, String query, boolean reconnect) throws Exception {
        // HACK
        this.enable_debug = true;
        LOG.info("Enabled debug output");
    }
    
    /**
     * 
     * @param paramStr
     * @return
     * @throws Exception
     */
    protected static List<String> extractParams(String paramStr) throws Exception {
        List<String> params = new ArrayList<String>();
        int pos = -1;
        int len = paramStr.length();
        while (++pos < len) {
            char cur = paramStr.charAt(pos);
            
            // Skip if it's just a space
            if (cur == ' ') continue;
            
            // See if our current position is a quotation mark
            // If it is, then we know that we have a string parameter
            if (cur == '"') {
                // Keep going until we reach an unescaped quotation mark
                boolean escaped = false;
                boolean valid = false;
                StringBuilder sb = new StringBuilder();
                while (++pos < len) {
                    cur = paramStr.charAt(pos); 
                    if (cur == '\\') {
                        escaped = true;
                    } else if (cur == '"' && escaped == false) {
                        valid = true;
                        break;
                    } else {
                        escaped = false;
                    }
                    sb.append(cur);
                } // WHILE
                if (valid == false) {
                    throw new Exception("Invalid parameter string '" + sb + "'");
                }
                params.add(sb.toString());

            // Otherwise just grab the substring to the next space 
            } else {
                int next = paramStr.indexOf(" ", pos);
                if (next == -1) {
                    params.add(paramStr.substring(pos));
                    pos = len;
                } else {
                    params.add(paramStr.substring(pos, next));
                    pos = next;
                }
            }
        }
        return (params);
    }
    
    private String formatResult(ClientResponse cr) {
        final VoltTable results[] = cr.getResults();
        final int num_results = results.length;
        StringBuilder sb = new StringBuilder();
        
        if (this.enable_debug) {
            sb.append(cr.toString());
        }
        else {
            // MAIN BODY
            if (this.enable_csv) {
                StringWriter out = new StringWriter();
                for (int i = 0; i < num_results; i++) {
                    if (i > 0) out.write("\n\n");
                    VoltTableUtil.csv(out, results[i], true);
                } // FOR
                sb.append(out.toString());
            } else {
                sb.append(VoltTableUtil.format(results));
            }
            
            // FOOTER
            String footer = "";
            if (this.enable_csv == false) {
                if (num_results == 1) {
                    int row_count = results[0].getRowCount(); 
                    footer = String.format("%d row%s in set", row_count, (row_count > 1 ? "s" : ""));
                }
                else if (num_results == 0) {
                    footer = "No results returned";
                }
                else {
                    footer = num_results + " tables returned";
                }
                sb.append(String.format("\n%s (%.2f sec)\n", footer, (cr.getClientRoundtrip() / 1000d)));
            }
        }
        return (sb.toString());
    }

    public static void main(String vargs[]) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs,
                ArgumentsParser.PARAM_CATALOG
        );
        HStoreTerminal term = new HStoreTerminal(args.catalog);
        
        // CSV OUTPUT
        if (args.hasBooleanParam(ArgumentsParser.PARAM_TERMINAL_CSV)) {
            term.enable_csv = args.getBooleanParam(ArgumentsParser.PARAM_TERMINAL_CSV);
        }
        // HOSTNAME
        if (args.hasParam(ArgumentsParser.PARAM_TERMINAL_HOST)) {
            term.hostname = args.getParam(ArgumentsParser.PARAM_TERMINAL_HOST);
        }
        // PORT
        if (args.hasParam(ArgumentsParser.PARAM_TERMINAL_PORT)) {
            term.port = args.getIntParam(ArgumentsParser.PARAM_TERMINAL_PORT);
        }
        
        term.run();
    }

}
