/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package edu.brown.api;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.conf.HStoreConf;

public class BenchmarkConfig {
    private static final Logger LOG = Logger.getLogger(BenchmarkConfig.class);
    
    public HStoreConf hstore_conf;
    public File hstore_conf_path;
    public String benchmark_conf_path;
    
    public String projectBuilderClass;
    
    public String backend;
    public String[] hosts;
    public int sitesPerHost;
    public int k_factor;
    public String[] clients;
    public String sshOptions[];
    public String remotePath;
    public String remoteUser;
    public boolean listenForDebugger;
    public int clientHeapSize;
    public boolean localmode;
    public float checkTransaction;
    public boolean checkTables;
    public String snapshotPath;
    public String snapshotPrefix;
    public String snapshotFrequency;
    public int snapshotRetain;
    
    public String statsDatabaseURL;
    public String statsDatabaseUser;
    public String statsDatabasePass;
    public String statsDatabaseJDBC;
    public int statsPollInterval;
    
    public String resultsDatabaseURL;
    public String statsTag;//Identifies the result set
    public String applicationName;
    public String subApplicationName;
    public String partitionPlanPath;
    
    public int client_initialPollingDelay;
    
    public boolean compileBenchmark;
    public boolean compileOnly;
    public boolean useCatalogHosts;
    public String workloadTrace;
    public Set<Integer> profileSiteIds;
    
    public boolean noSites;
    public boolean noLoader;
    public boolean noUploading;
    public boolean noExecute;
    public boolean noShutdown;
    
    public boolean killOnZeroResults;
    
    public String markovPath;
    public String markov_thresholdsPath;
    public Double markov_thresholdsValue;
    public boolean markovRecomputeAfterEnd;
    public boolean markovRecomputeAfterWarmup;
    
    public String evictable[];
    public String deferrable[];
	public String batchEvictable[];
	
    public boolean dumpDatabase;
    public String dumpDatabaseDir;
    
    public final Map<String, String> clientParameters = new TreeMap<String, String>();
    public final Map<String, String> siteParameters = new TreeMap<String, String>();

    private PropertiesConfiguration config = null;


    
    /**
     * 
     * @param benchmark_conf_path
     */
    public BenchmarkConfig(File benchmark_conf_path) {
        this.loadConfigFile(benchmark_conf_path);
    }

    @SuppressWarnings("unchecked")
    public void loadConfigFile(File path) {
        try {
            this.config = new PropertiesConfiguration(benchmark_conf_path);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to load benchmark configuration file " + benchmark_conf_path);
        }
        
        Class<?> confClass = this.getClass();
        for (Object key : CollectionUtil.iterable(this.config.getKeys())) {
            String f_name = key.toString();
            String f_value = this.config.getString(f_name); 
            
            // Always store whatever the property as a client parameter
            String paramName = (HStoreConstants.BENCHMARK_PARAM_PREFIX + f_name).toUpperCase();
            LOG.debug(String.format("Passing benchmark parameter to clients: %s = %s", paramName, f_value));
            clientParameters.put(paramName, f_value);
            
            Field f = null;
            try {
                f = confClass.getField(f_name);
            } catch (Exception ex) {
                // XXX LOG.warn("Invalid configuration property '" + f_name + "'. Ignoring...");
                continue;
            }
            assert(f != null);
            
            Class<?> f_class = f.getType();
            Object value = null;
            
            if (f_class.equals(int.class)) {
                value = this.config.getInt(f_name);
            } else if (f_class.equals(long.class)) {
                value = this.config.getLong(f_name);
            } else if (f_class.equals(double.class)) {
                value = this.config.getDouble(f_name);
            } else if (f_class.equals(boolean.class)) {
                value = this.config.getBoolean(f_name);
            } else if (f_class.equals(String.class)) {
                value = this.config.getString(f_name);
            } else {
                LOG.warn(String.format("Unexpected value type '%s' for property '%s'", f_class.getSimpleName(), f_name));
            }
            
            try {
                f.set(this, value);
                LOG.info(String.format("SET %s = %s", f_name, value));
            } catch (Exception ex) {
                throw new RuntimeException("Failed to set value '" + value + "' for field '" + f_name + "'", ex);
            }
        } // FOR
    }
    
    public BenchmarkConfig(
            HStoreConf hstore_conf,
            File hstore_conf_path,
            String benchmark_conf_path,
            String benchmarkClient,
            String backend,
            String[] hosts,
            int sitesPerHost,
            int kFactor,
            String[] clients,
            String sshOptions,
            String remotePath,
            String remoteUser,
            boolean listenForDebugger,
            int serverHeapSize,
            int clientHeapSize,
            int client_polling_delay,
            boolean localmode,
            float checkTransaction,
            boolean checkTables,
            String snapshotPath,
            String snapshotPrefix,
            String snapshotFrequency,
            int snapshotRetain,
            String statsDatabaseURL,
            String statsDatabaseUser,
            String statsDatabasePass,
            String statsDatabaseJDBC,
            int statsPollInterval,
            String statsTag,
            String applicationName,
            String subApplicationName,
            boolean compileBenchmark,
            boolean compileOnly,
            boolean useCatalogHosts,
            boolean noSites,
            boolean noLoader,
            boolean noUploading,
            boolean noExecute,
            boolean noShutdown,
            boolean killOnZeroResults,
            String workloadTrace,
            Set<Integer> profileSiteIds,
            String partitionPlanPath,
            String markovPath,
            String thresholdsPath,
            Double thresholdsValue,
            boolean markovRecomputeAfterEnd,
            boolean markovRecomputeAfterWarmup,
            String evictable[],
            String batchEvictable[],
            String deferrable[],
            boolean dumpDatabase,
            String dumpDatabaseDir
        ) {
        
        this.projectBuilderClass = benchmarkClient;
        this.backend = backend;
        this.hosts = new String[hosts.length];
        for (int i = 0; i < hosts.length; i++)
            this.hosts[i] = hosts[i];
        this.sitesPerHost = sitesPerHost;
        this.k_factor = kFactor;
        this.clients = new String[clients.length];
        for (int i = 0; i < clients.length; i++)
            this.clients[i] = clients[i];
        this.sshOptions = sshOptions.split(" "); // HACK
        this.remotePath = remotePath;
        this.remoteUser = remoteUser;
        this.listenForDebugger = listenForDebugger;
        this.clientHeapSize = clientHeapSize;
        this.client_initialPollingDelay = client_polling_delay;
        this.localmode = localmode;
        this.checkTransaction = checkTransaction;
        this.checkTables = checkTables;
        this.snapshotPath = snapshotPath;
        this.snapshotPrefix = snapshotPrefix;
        this.snapshotFrequency = snapshotFrequency;
        this.snapshotRetain = snapshotRetain;
        
        // FIXME this.resultsDatabaseURL = resultsDatabaseURL;
        
        this.statsDatabaseURL = statsDatabaseURL;
        this.statsDatabaseUser = statsDatabaseUser;
        this.statsDatabasePass = statsDatabasePass;
        this.statsDatabaseJDBC = statsDatabaseJDBC;
        this.statsTag = statsTag;
        this.statsPollInterval = statsPollInterval;
        
        this.applicationName = applicationName;
        this.subApplicationName = subApplicationName;
        
        this.compileBenchmark = compileBenchmark;
        this.compileOnly = compileOnly;
        this.useCatalogHosts = useCatalogHosts;
        
        this.noSites = noSites;
        this.noLoader = noLoader;
        this.noUploading = noUploading;
        this.noExecute = noExecute;
        this.noShutdown = noShutdown;
        
        this.killOnZeroResults = killOnZeroResults;
        
        this.workloadTrace = workloadTrace;
        this.profileSiteIds = profileSiteIds;
        
        this.partitionPlanPath = partitionPlanPath;
        this.markovPath = markovPath;
        this.markov_thresholdsPath = thresholdsPath;
        this.markov_thresholdsValue = thresholdsValue;
        this.markovRecomputeAfterEnd = markovRecomputeAfterEnd;
        this.markovRecomputeAfterWarmup = markovRecomputeAfterWarmup;
        
        this.evictable = evictable;
        this.batchEvictable = batchEvictable;
        this.deferrable = deferrable;
        
        this.dumpDatabase = dumpDatabase;
        this.dumpDatabaseDir = dumpDatabaseDir;
        
        this.hstore_conf = hstore_conf;
        this.hstore_conf_path = hstore_conf_path;
        this.benchmark_conf_path = benchmark_conf_path;
        if (this.benchmark_conf_path.isEmpty() == false) {
            this.loadConfigFile(new File(this.benchmark_conf_path));
        }
    }

    @Override
    public String toString() {
        Class<?> confClass = this.getClass();
        final Map<String, Object> m0 = new ListOrderedMap<String, Object>();
        final Map<String, Object> m1 = new ListOrderedMap<String, Object>();
        final Map<String, Object> m2 = new ListOrderedMap<String, Object>();
        
        for (Field f : confClass.getFields()) {
            String key = f.getName().toUpperCase();
            if (key.equalsIgnoreCase("hosts")) {
                m0.put("Number of Hosts", this.hosts.length);
                m0.put("Hosts", StringUtil.join("\n", this.hosts));
            } else if (key.equalsIgnoreCase("clients")) {
                m1.put("Number of Clients", this.clients.length);
                m1.put("Clients", StringUtil.join("\n", this.clients));
            } else if (key.equalsIgnoreCase("clientParameters") || key.equalsIgnoreCase("siteParameters")) {
                // Skip
            } else {
                Object val = null;
                try {
                    val = f.get(this);
                    if (ClassUtil.isArray(val)) val = Arrays.toString((Object[])val);
                } catch (IllegalAccessException ex) {
                    val = ex.getMessage();
                }
                if ((val instanceof HStoreConf) == false) m2.put(key, val);
            }
        } // FOR
        return (StringUtil.formatMaps(m0, m1, this.clientParameters, this.siteParameters, m2));
    }
}
