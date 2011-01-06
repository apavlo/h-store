package edu.brown.markov;

import java.io.File;
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

/**
 * 
 * @author pavlo
 */
public class TransactionFeatureExtractor {
    private static final Logger LOG = Logger.getLogger(TransactionFeatureExtractor.class);

    private final Database catalog_db;
    private final PartitionEstimator p_estimator;
    private final Map<Procedure, List<AbstractFeature>> proc_features = new HashMap<Procedure, List<AbstractFeature>>();
    
    private static final Class<?> DEFAULT_FEATURE_CLASSES[] = new Class<?>[] {
        TransactionIdFeature.class,
        BasePartitionFeature.class,
        ParamNumericValuesFeature.class,
        ParamArrayAllSameHashFeature.class,
        ParamHashPartitionFeature.class,
        ParamArrayLengthFeature.class,
        ParamHashEqualsBasePartitionFeature.class
    };

    /**
     * Full Constructor
     * @param catalog_db
     * @param feature_classes
     */
    public TransactionFeatureExtractor(Database catalog_db, PartitionEstimator p_estimator, Class<? extends AbstractFeature>...feature_classes) {
        this.catalog_db = catalog_db;
        this.p_estimator = p_estimator;
        for (Class<? extends AbstractFeature> fclass : feature_classes) {
            this.addFeatureClass(fclass);
        } // FOR
    }
    
    public TransactionFeatureExtractor(Database catalog_db, Class<? extends AbstractFeature>...feature_classes) {
        this(catalog_db, new PartitionEstimator(catalog_db), feature_classes);
    }
    
    /**
     * Constructor
     * @param catalog_db
     */
    @SuppressWarnings("unchecked")
    public TransactionFeatureExtractor(Database catalog_db) {
        this(catalog_db, (Class<? extends AbstractFeature>[])DEFAULT_FEATURE_CLASSES);
    }
    
    /**
     * Add a feature class to this extractor
     * @param feature_class
     */
    public void addFeatureClass(Class<? extends AbstractFeature> feature_class) {
        assert(feature_class != null);
        if (LOG.isDebugEnabled()) LOG.debug("Adding " + feature_class.getSimpleName());

        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            if (catalog_proc.getSystemproc()) continue;
            if (!this.proc_features.containsKey(catalog_proc)) {
                this.proc_features.put(catalog_proc, new ArrayList<AbstractFeature>());
            }
            AbstractFeature f = (AbstractFeature)ClassUtil.newInstance(
                                    feature_class,
                                    new Object[]{ this.p_estimator, catalog_proc },
                                    new Class[] { PartitionEstimator.class, Procedure.class });
            this.proc_features.get(catalog_proc).add(f);
        } // fOR
    }
    
    /**
     * 
     * @param workload
     * @return
     */
    public Map<Procedure, FeatureSet> calculate(Workload workload) throws Exception {
        Map<Procedure, FeatureSet> fsets = new HashMap<Procedure, FeatureSet>();
        
        for (TransactionTrace txn_trace : workload.getTransactions()) {
            final boolean trace = LOG.isTraceEnabled();
            if (trace) LOG.trace("Processing " + txn_trace);
            
            Procedure catalog_proc = txn_trace.getCatalogItem(this.catalog_db);
            assert(catalog_proc != null) : "Invalid procedure: " + txn_trace.getCatalogItemName();
            FeatureSet fset = fsets.get(catalog_proc);
            if (fset == null) {
                fset = new FeatureSet();
                fsets.put(catalog_proc, fset);
            }
            
            for (AbstractFeature f : this.proc_features.get(catalog_proc)) {
                LOG.trace(txn_trace + " - " + f.getClass().getSimpleName());
                f.extract(fset, txn_trace);
            }
            
            if (trace) LOG.trace(txn_trace + ": " + fset.getFeatureValues(txn_trace));
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
            File path = new File(proc_name + ".arff");
            Instances data = e.getValue().export(proc_name, true);
            FileUtil.writeStringToFile(path, data.toString());
            LOG.info(String.format("Wrote FeatureSet with %d instances to '%s'", data.numInstances(), path.getAbsolutePath()));
            
            TransactionClusterer txn_c = new TransactionClusterer(args.catalog_db, args.workload);
            txn_c.calculate(e.getValue(), e.getKey());
        }
        
    }
}