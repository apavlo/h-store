package edu.brown.stream;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltSystemProcedure;
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
import org.voltdb.client.ProcCallException;
import org.voltdb.sysprocs.Statistics;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.VoltTableUtil;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.CollectionUtil;


public class MyClient {
  final int port;
  Catalog catalog;
  Client client;
  InputClientConnection icc;

  MyClient() {
    this.port = 21212;
    this.catalog = new Catalog();
    this.icc = this.getClientConnection("localhost");
    this.client = icc.client;
  }

  private class InputClientConnection {
        final Client client;
        final String hostname;
        final int port;

        public InputClientConnection(Client client, String hostname, int port) {
            this.client = client;
            this.hostname = hostname;
            this.port = port;
        }
    } // CLASS

    private InputClientConnection getClientConnection(String host) {
        String hostname = null;
        int port = -1;

        // Fixed hostname
        if (host != null) {
            if (host.contains(":")) {
                String split[] = host.split("\\:", 2);
                hostname = split[0];
                port = Integer.valueOf(split[1]);
            } else {
                hostname = host;
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

        /*if (debug.val)
            LOG.debug(String.format("Creating new client connection to %s:%d",
                      hostname, port));*/
        System.out.println(String.format("Creating new client connection to %s:%d",
                      hostname, port));
        Client client = ClientFactory.createClient(128, null, false, null);
        try {
            client.createConnection(null, hostname, port, "user", "password");
            System.out.println("BatchRunner: connection is ok ... ");
        } catch (Exception ex) {
            String msg = String.format("Failed to connect to HStoreSite at %s:%d", hostname, port);
            throw new RuntimeException(msg);
        }
        return new InputClientConnection(client, hostname, port);
    }

  public static void main(String [] args) {
    MyClient myc = new MyClient();
    try {
        if (myc.client == null) {
            System.out.println("Everything is terrible");
        }
        else {
            //myc.client.callProcedure("SimpleCall");
            //myc.client.callProcedure("Initialize");
            //myc.client.callProcedure("SignUp", 3);
            myc.client.callProcedure("CheckoutBike", 3);
            //myc.client.callProcedure("CheckoutBike", 111111);
        }
    } catch (NoConnectionsException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    } catch (ProcCallException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }
    System.out.print("hello");
  }
  
}