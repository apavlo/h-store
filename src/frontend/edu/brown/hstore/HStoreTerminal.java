package edu.brown.hstore;

import jline.*;

import java.io.*;
import java.util.*;
import java.util.zip.*;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Catalog;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.protorpc.AbstractEventHandler;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;

/** MySQL Terminal **/
public class HStoreTerminal implements Runnable { //extends AbstractEventHandler??
	public static final Logger LOG = Logger.getLogger(HStoreTerminal.class);
	
	final Catalog catalog;
	final Client client;
	final jline.ConsoleReader reader = new ConsoleReader(); 
	
	public HStoreTerminal(Catalog catalog) throws Exception{
		this.catalog = catalog;
		this.client = this.getClientConnection();
		usage();
	}
	
	public static void usage() {
        System.out.println("Usage: At prompt, type in ad hoc SQL command. Results will be printed to terminal. ");
    }
	
	
	private Client getClientConnection() {
        // Connect to random host and using a random port that it's listening on
		Map<Integer, Set<Pair<String, Integer>>> hosts = CatalogUtil.getExecutionSites(this.catalog);
        Integer site_id = CollectionUtil.random(hosts.keySet());
        assert(site_id != null);
        Pair<String, Integer> p = CollectionUtil.random(hosts.get(site_id));
        assert(p != null);
        LOG.debug(String.format("Creating new client connection to HStoreSite %s", HStoreThreadManager.formatSiteName(site_id)));
        
        Client new_client = ClientFactory.createClient(128, null, false, null);
        try {
            new_client.createConnection(null, p.getFirst(), p.getSecond(), "user", "password");
        } catch (Exception ex) {
            throw new RuntimeException(String.format("Failed to connect to HStoreSite %s at %s:%d",
                                                     HStoreThreadManager.formatSiteName(site_id), p.getFirst(), p.getSecond()));
        }
        return (new_client);
    }
	
	@Override
	public void run() {
		String query = "";
		ClientResponse cresponse = null;
		do {
			try {
				query = reader.readLine("hstore> ");
				cresponse = this.client.callProcedure("@AdHoc", query);
				VoltTable[] results = cresponse.getResults();
				System.out.println(results[0].toString());
			} catch (ProcCallException ex) {
				LOG.error(ex.getMessage());
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
        } while(query != null && query.length() > 0);
	}
	
	public static void main(String vargs[]) throws Exception {
		ArgumentsParser args = ArgumentsParser.load(vargs,
                ArgumentsParser.PARAM_CATALOG
		);
		//Ask Andy... is this right?
		HStoreTerminal term = new HStoreTerminal(args.catalog);
		term.run();
	}

}
