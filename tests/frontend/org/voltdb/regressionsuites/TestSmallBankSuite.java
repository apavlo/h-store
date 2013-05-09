package org.voltdb.regressionsuites;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import edu.brown.benchmark.smallbank.SmallBankConstants;
import edu.brown.benchmark.smallbank.SmallBankLoader;
import edu.brown.benchmark.smallbank.SmallBankProjectBuilder;
import edu.brown.benchmark.smallbank.procedures.SendPayment;
import edu.brown.hstore.Hstoreservice.Status;

/**
 * Simple test suite for the SmallBank benchmark
 * @author pavlo
 */
public class TestSmallBankSuite extends RegressionSuite {

    private static final String PREFIX = "smallbank";
    private static final double SCALEFACTOR = 0.001;
    
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestSmallBankSuite(String name) {
        super(name);
    }
    
    private void checkBalance(Client client, long acctId, double expected) throws Exception {
        // Make sure that we set it correctly
        String query = String.format("SELECT * FROM %s WHERE custid = %d",
                              SmallBankConstants.TABLENAME_CHECKING, acctId);
        ClientResponse cresponse = client.callProcedure("@AdHoc", query);
        assertEquals(Status.OK, cresponse.getStatus());
        VoltTable results[] = cresponse.getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].getRowCount());
        assertTrue(results[0].advanceRow());
        assertEquals(expected, results[0].getDouble("bal"));
    }
    
    private void updateBalance(Client client, long acctId, double balance) throws Exception {
        // Prime the customer's balance
        String query = String.format("UPDATE %s SET bal = %f WHERE custid = %d",
                              SmallBankConstants.TABLENAME_CHECKING,
                              balance, acctId);
        ClientResponse cresponse = client.callProcedure("@AdHoc", query);
        assertEquals(Status.OK, cresponse.getStatus());
        VoltTable results[] = cresponse.getResults();
        assertEquals(1, results.length);
        assertTrue(results[0].advanceRow());
    }

    /**
     * testSendPayment
     */
    public void testSendPayment() throws Exception {
        CatalogContext catalogContext = this.getCatalogContext();
        Client client = this.getClient();
        TestSmallBankSuite.initializeSmallBankDatabase(catalogContext, client);
        
        long acctIds[] = { 1l, 2l };
        double balances[] = { 100d, 0d };
        ClientResponse cresponse;
        VoltTable results[];
        
        for (int i = 0; i < acctIds.length; i++) {
            this.updateBalance(client, acctIds[i], balances[i]);
            this.checkBalance(client, acctIds[i], balances[i]);
        } // FOR
        
        // Run the SendPayment txn to send all the money from the first
        // account to the second account.
        cresponse = client.callProcedure(SendPayment.class.getSimpleName(),
                                         acctIds[0], acctIds[1], balances[0]);
        assertEquals(Status.OK, cresponse.getStatus());
        results = cresponse.getResults();
        assertEquals(2, results.length);
        for (int i = 0; i < results.length; i++) {
            assertEquals(1l, results[i].asScalarLong());
            // System.err.println(VoltTableUtil.format(results[i]));
        } // FOR
        
        // Make sure the account balances have switched
        for (int i = 0; i < acctIds.length; i++) {
            this.checkBalance(client, acctIds[i], balances[i == 0 ? 1 : 0]);
        } // FOR
    }
    
    /**
     * testSendPaymentInsufficientFunds
     */
    public void testSendPaymentInsufficientFunds() throws Exception {
        CatalogContext catalogContext = this.getCatalogContext();
        Client client = this.getClient();
        TestSmallBankSuite.initializeSmallBankDatabase(catalogContext, client);
        
        long acctIds[] = { 1l, 2l };
        double balances[] = { 0d, 100d };
        ClientResponse cresponse;
        
        for (int i = 0; i < acctIds.length; i++) {
            this.updateBalance(client, acctIds[i], balances[i]);
            this.checkBalance(client, acctIds[i], balances[i]);
        } // FOR
        
        // Run the SendPayment txn that tries to send to money from the first account,
        // but it should fail because the balance is zero
        try {
            cresponse = client.callProcedure(SendPayment.class.getSimpleName(),
                                             acctIds[0], acctIds[1], balances[1]);
        } catch (ProcCallException ex) {
            cresponse = ex.getClientResponse();
        }
        assertEquals(Status.ABORT_USER, cresponse.getStatus());
        
        // Make sure the account balances are still the same
        for (int i = 0; i < acctIds.length; i++) {
            this.checkBalance(client, acctIds[i], balances[i]);
        } // FOR
    }
    
    public static final void initializeSmallBankDatabase(final CatalogContext catalogContext, final Client client) throws Exception {
        String args[] = {
            "NOCONNECTIONS=true",
            "CLIENT.SCALEFACTOR=" + SCALEFACTOR,
        };
        SmallBankLoader loader = new SmallBankLoader(args) {
            {
                this.setCatalogContext(catalogContext);
                this.setClientHandle(client);
            }
            @Override
            public CatalogContext getCatalogContext() {
                return (catalogContext);
            }
        };
        loader.load();
    }

    public static Test suite() {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestSmallBankSuite.class);
        SmallBankProjectBuilder project = new SmallBankProjectBuilder();
        project.addAllDefaults();
        
        boolean success;
        VoltServerConfig config;
        
        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partition running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalSingleProcessServer(PREFIX+"-1part.jar", 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);
        
        /////////////////////////////////////////////////////////////
        // CONFIG #2: 1 Local Site with 2 Partitions running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalSingleProcessServer(PREFIX+"-2part.jar", 2, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        ////////////////////////////////////////////////////////////
        // CONFIG #3: cluster of 2 nodes running 2 site each, one replica
        ////////////////////////////////////////////////////////////
        config = new LocalCluster(PREFIX+"-cluster.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }

}
