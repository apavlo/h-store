package edu.brown.markov;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.voltdb.catalog.*;

import weka.core.Instances;

import edu.brown.markov.features.*;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;

public class TransactionFeatureExtractor {
    private static final Logger LOG = Logger.getLogger(TransactionFeatureExtractor.class);

    private final Database catalog_db;
    private final PartitionEstimator p_estimator;
    private final Map<Procedure, List<AbstractFeature>> proc_features = new HashMap<Procedure, List<AbstractFeature>>();
    
    private static final Class<?> FEATURE_CLASSES[] = new Class<?>[] {
//        TransactionIdFeature.class,
        BasePartitionFeature.class,
        ArrayLengthFeature.class,
        ParameterHashEqualsBasePartitionFeature.class
    };
    
    /**
     * Constructor
     * @param catalog_db
     */
    public TransactionFeatureExtractor(Database catalog_db) {
        this.catalog_db = catalog_db;
        this.p_estimator = new PartitionEstimator(catalog_db);
        
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            if (catalog_proc.getSystemproc()) continue;

            // Instantiate new AbstractFeature objects for each procedure
            List<AbstractFeature> features = new ArrayList<AbstractFeature>();
            for (Class<?> featureClass: FEATURE_CLASSES) {
                AbstractFeature f = (AbstractFeature)ClassUtil.newInstance(featureClass,
                                                                           new Object[]{ this.p_estimator, catalog_proc },
                                                                           new Class[] { PartitionEstimator.class, Procedure.class });
                features.add(f);
            } // FOR
            this.proc_features.put(catalog_proc, features);
        } // FOR
    }
    
    /**
     * 
     * @param workload
     * @return
     */
    public Map<Procedure, FeatureSet> calculate(Workload workload) throws Exception {
        Map<Procedure, FeatureSet> fsets = new HashMap<Procedure, FeatureSet>();
        
        for (TransactionTrace txn_trace : workload.getTransactions()) {
            LOG.debug("Processing " + txn_trace);
            
            Procedure catalog_proc = txn_trace.getCatalogItem(this.catalog_db);
            assert(catalog_proc != null) : "Invalid procedure: " + txn_trace.getCatalogItemName();
            FeatureSet fset = fsets.get(catalog_proc);
            if (fset == null) {
                fset = new FeatureSet();
                fsets.put(catalog_proc, fset);
            }
            
            for (AbstractFeature f : this.proc_features.get(catalog_proc)) {
                LOG.trace(txn_trace + " - " + f.getClass().getSimpleName());
                f.calculate(fset, txn_trace);
            }
            
            if (LOG.isDebugEnabled()) {
                LOG.debug(txn_trace + ": " + fset.getFeatures(txn_trace));
            }
        } // FOR
        return (fsets);
    }

    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(
            ArgumentsParser.PARAM_CATALOG,
            ArgumentsParser.PARAM_WORKLOAD
        );
        
        TransactionFeatureExtractor extractor = new TransactionFeatureExtractor(args.catalog_db);
        Map<Procedure, FeatureSet> fsets = extractor.calculate(args.workload);
        
        for (Entry<Procedure, FeatureSet> e : fsets.entrySet()) {
            String proc_name = e.getKey().getName();
            String path = "/tmp/" + proc_name + ".arff";
            Instances data = e.getValue().export(proc_name);
            FileUtil.writeStringToFile(path, data.toString());
        }
        
    }
}