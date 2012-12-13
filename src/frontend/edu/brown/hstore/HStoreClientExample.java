package edu.brown.hstore;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;

import edu.brown.utils.ArgumentsParser;

public class HStoreClientExample {
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG_HOSTS);
        
        // connect to VoltDB server
        Client client = ClientFactory.createClient();
        client.createConnection(null, args.getParam(ArgumentsParser.PARAM_CATALOG_HOSTS), HStoreConstants.DEFAULT_PORT, "user", "password");

        // long w_id, String w_name, String w_street_1, String w_street_2, String w_city, String w_state, String w_zip, double w_tax, double w_ytd
        VoltTable[] result = client.callProcedure("InsertSubscriber", 1l, "0000001").getResults();
        
//        VoltTable[] result = client.callProcedure("InsertWarehouse",
//                0l,
//                "w_name",
//                "w_street_1",
//                "w_street_2",
//                "w_city",
//                "ws",
//                "w_zip",
//                0.0d,
//                0.0d
//        );
//        VoltTable[] result = client.callProcedure("InsertProcedure", 42L, 99L);
        System.out.println("result length = " + result.length);

        // client.shutdown();
    }
}
