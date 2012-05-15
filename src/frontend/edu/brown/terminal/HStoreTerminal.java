package edu.brown.terminal;

import jline.ConsoleReader;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;

/**
 * MySQL Terminal
 * @author gen
 * @author pavlo
 */
public class HStoreTerminal implements Runnable { //extends AbstractEventHandler??
    public static final Logger LOG = Logger.getLogger(HStoreTerminal.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    
    private static final String PROMPT = "hstore> ";
    
    final Catalog catalog;
    final jline.ConsoleReader reader = new ConsoleReader(); 
    final TokenCompletor completer;
    
    public HStoreTerminal(Catalog catalog) throws Exception{
        this.catalog = catalog;
        
        if (debug.get()) LOG.debug("Generating tab-completion keywords");
        this.completer = new TokenCompletor(catalog);
        this.reader.addCompletor(this.completer);
    }
    
    public void printHeader() {
        System.out.println(" _  _     ___ _____ ___  ___ ___"); 
        System.out.println("| || |___/ __|_   _/ _ \\| _ \\ __|");
        System.out.println("| __ |___\\__ \\ | || (_) |   / _|"); 
        System.out.println("|_||_|   |___/ |_| \\___/|_|_\\___|");
        System.out.println();
//        System.out.println("Usage: At prompt, type in ad hoc SQL command. Results will be printed to terminal. ");
    }
    
    
    private Pair<Client, Site> getClientConnection() {
        // Connect to random host and using a random port that it's listening on
        Site catalog_site = CollectionUtil.random(CatalogUtil.getAllSites(this.catalog));
        Host catalog_host = catalog_site.getHost();
        
        String hostname = catalog_host.getIpaddr();
        int port = catalog_site.getProc_port();
        if (debug.get()) LOG.debug(String.format("Creating new client connection to HStoreSite %s",
                                HStoreThreadManager.formatSiteName(catalog_site.getId())));
        
        Client new_client = ClientFactory.createClient(128, null, false, null);
        try {
            new_client.createConnection(null, hostname, port, "user", "password");
        } catch (Exception ex) {
            throw new RuntimeException(String.format("Failed to connect to HStoreSite %s at %s:%d",
                                                     HStoreThreadManager.formatSiteName(catalog_site.getId()),
                                                     hostname, port));
        }
        return Pair.of(new_client, catalog_site);
    }
    
    @Override
    public void run() {
        this.printHeader();
        
        Pair<Client, Site> p = this.getClientConnection();
        Client client = p.getFirst();
        Site catalog_site = p.getSecond();
        System.out.printf("Connected to %s:%d / Version: %s\n",
                          catalog_site.getHost().getIpaddr(),
                          catalog_site.getProc_port(),
                          client.getBuildString());
//        System.out.printf("ClusterId: %s / Build Version: %s\n",
//                          client.getInstanceId().hashCode(), );
        
        String query = "";
        ClientResponse cresponse = null;
        try {
            do {
                try {
                    query = reader.readLine(PROMPT);
                    if (query == null || query.isEmpty()) continue;
                    
                    if (debug.get()) LOG.debug("QUERY: " + query);
                    cresponse = client.callProcedure("@AdHoc", query);
                    VoltTable[] results = cresponse.getResults();
                    System.out.println(results[0].toString());
                } catch (ProcCallException ex) {
                    LOG.error(ex.getMessage());
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            } while(query != null); //TODO: Note to Andy, should there be an exit sequence or escape character?
        } finally {
            try {
                client.close();
            } catch (InterruptedException ex) {
                // Ignore
            }
        }
    }
    
    public static void main(String vargs[]) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs,
                ArgumentsParser.PARAM_CATALOG
        );
        
        HStoreTerminal term = new HStoreTerminal(args.catalog);
        term.run();
    }

}
