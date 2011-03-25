package edu.mit;

import org.voltdb.BackendTarget;
import org.voltdb.ExecutionSite;
import org.voltdb.VoltProcedure;
import org.voltdb.client.ClientResponse;

import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.PartitionEstimator;
import edu.mit.hstore.dtxn.LocalTransactionState;
import edu.mit.hstore.dtxn.TransactionState;

public class VoltProcedureInvoker {

    //private final static Object[] EMPTY_ARRAY = {};
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        if (args.catalog_db == null) {
            System.err.println("VoltProcedureInvoker " + ArgumentsParser.PARAM_CATALOG_JAR + "=<catalog jar>");
            System.exit(1);
        }
        Integer local_partition = args.getIntParam(ArgumentsParser.PARAM_SIMULATOR_PARTITION);
        assert(local_partition != null) : "Must pass in the local partition id";

        // Partition Estimator
        PartitionEstimator p_estimator = new PartitionEstimator(args.catalog_db, args.hasher);
        
        // setup the EE
        ExecutionSite executor = new ExecutionSite(local_partition, args.catalog, BackendTarget.NATIVE_EE_JNI, p_estimator, null);
        VoltProcedure procedure = executor.getVoltProcedure("EmptyProcedure");

        // Error: EmptyProcedure is supposed to take one argument
        ClientResponse result = null; // procedure.call(EMPTY_ARRAY);
//        System.out.println("status = " + result.getStatus());
//        System.out.println("result length = " + result.getResults().length);

//        result = procedure.call(42L);
//        System.out.println("status = " + result.getStatus());
//        System.out.println("result length = " + result.getResults().length);

        procedure = executor.getVoltProcedure("InsertProcedure");
        for (long i = 0; i < 10; i++) {
            TransactionState txnState = new LocalTransactionState(executor);
            txnState.init(i, 123, 0);
            result = procedure.callAndBlock(txnState, 100L);
            System.out.println("[" + i + "] Insert status = " + result.getStatus());
            System.out.println("[" + i + "] Insert result (rows affected) = " + result.getResults()[0].asScalarLong());
        }
    }
}
