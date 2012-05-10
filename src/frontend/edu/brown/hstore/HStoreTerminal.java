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
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.protorpc.AbstractEventHandler;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;

/** MySQL Terminal **/
public class HStoreTerminal{ //extends AbstractEventHandler??
	public static final Logger LOG = Logger.getLogger(HStoreTerminal.class);
	
	final Catalog catalog;
	final Client client;
	
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
	
	
	public static void main(String vargs[]) throws Exception {
		ArgumentsParser args = ArgumentsParser.load(vargs,
                ArgumentsParser.PARAM_CATALOG
		);
		HStoreTerminal term = new HStoreTerminal(args.catalog);
		jline.ConsoleReader reader = new ConsoleReader(); 
		String query = "";
		do {
			query = reader.readLine("enter command > ");
			ClientResponse cresponse = term.client.callProcedure("@AdHoc", query);
			VoltTable[] results = cresponse.getResults();
			System.out.println(results[0].toString());
        } while(query != null && query.length() > 0);
	}

}
