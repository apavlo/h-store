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

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;

/**
 * MySQL Terminal
 * @author gen
 */
public class HStoreTerminal implements Runnable { //extends AbstractEventHandler??
    public static final Logger LOG = Logger.getLogger(HStoreTerminal.class);
    
    private static final String PROMPT = "hstore> ";
    
    final Catalog catalog;
    final Client client;
    final jline.ConsoleReader reader = new ConsoleReader(); 
    final TokenCompletor completer;
    
    public HStoreTerminal(Catalog catalog) throws Exception{
        this.catalog = catalog;
        this.client = this.getClientConnection();
        
        LOG.info("Generating tab-completion keywords");
        this.completer = new TokenCompletor(catalog);
        this.reader.addCompletor(this.completer);
        usage();
    }
    
    public static void usage() {
        System.out.println("Usage: At prompt, type in ad hoc SQL command. Results will be printed to terminal. ");
    }
    
    
    private Client getClientConnection() {
        // Connect to random host and using a random port that it's listening on
        Site catalog_site = CollectionUtil.random(CatalogUtil.getAllSites(this.catalog));
        Host catalog_host = catalog_site.getHost();
        
        String hostname = catalog_host.getIpaddr();
        int port = catalog_site.getProc_port();
        LOG.debug(String.format("Creating new client connection to HStoreSite %s",
                                HStoreThreadManager.formatSiteName(catalog_site.getId())));
        
        Client new_client = ClientFactory.createClient(128, null, false, null);
        try {
            new_client.createConnection(null, hostname, port, "user", "password");
        } catch (Exception ex) {
            throw new RuntimeException(String.format("Failed to connect to HStoreSite %s at %s:%d",
                                                     HStoreThreadManager.formatSiteName(catalog_site.getId()),
                                                     hostname, port));
        }
        return (new_client);
    }
    
    @Override
    public void run() {
        String query = "";
        ClientResponse cresponse = null;
        do {
            try {
                query = reader.readLine(PROMPT);
                LOG.info("QUERY: " + query);
//                cresponse = this.client.callProcedure("@AdHoc", query);
//                VoltTable[] results = cresponse.getResults();
//                System.out.println(results[0].toString());
//            } catch (ProcCallException ex) {
//                LOG.error(ex.getMessage());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        } while(query != null); //TODO: Note to Andy, should there be an exit sequence or escape character?
    }
    
    public static void main(String vargs[]) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs,
                ArgumentsParser.PARAM_CATALOG
        );
        
        HStoreTerminal term = new HStoreTerminal(args.catalog);
        term.run();
    }

}
