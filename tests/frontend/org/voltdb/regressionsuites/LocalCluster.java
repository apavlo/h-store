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
package org.voltdb.regressionsuites;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.ServerThread;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Site;
import org.voltdb.compiler.VoltProjectBuilder;

import edu.brown.catalog.CatalogInfo;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.ClusterConfiguration;
import edu.brown.catalog.FixCatalog;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;

/**
 * Implementation of a VoltServerConfig for a multi-process
 * cluster. All cluster processes run locally (keep this in
 * mind if building memory or load intensive tests.)
 */
public class LocalCluster extends VoltServerConfig {
    private static final Logger LOG = Logger.getLogger(LocalCluster.class);

    // configuration data
    final File m_jarFileName;
    final int m_partitionPerSite;
    final int m_siteCount;
    final int m_replication;
    final BackendTarget m_target;
    final String m_buildDir;
    int m_portOffset;
    CatalogContext catalogContext;

    // state
    boolean m_compiled = false;
    boolean m_running = false;
    ArrayList<Process> m_cluster = null;
    ArrayList<PipeToFile> m_pipes = null;
    ServerThread m_localServer = null;

    // components
    ProcessBuilder m_procBuilder;

    /* class pipes a process's output to a file name.
     * Also watches for "Server completed initialization"
     * in output - the sygil of readiness!
     */
    public static class PipeToFile implements Runnable {
        FileWriter m_writer ;
        InputStream m_input;
        String m_filename;

        // set m_witnessReady when the m_token byte sequence is seen.
        AtomicBoolean m_witnessedReady;
        
        final String msg = HStoreConstants.SITE_READY_MSG;
        
        final int m_token[] = new int[msg.length()]; {
            for (int i = 0; i < msg.length(); ++i) {
                m_token[i] = msg.charAt(i);
            }
        }

        PipeToFile(String filename, InputStream stream) {
            m_witnessedReady = new AtomicBoolean(false);
            m_filename = filename;
            m_input = stream;
            try {
                m_writer = new FileWriter(filename, true);
            }
            catch (IOException ex) {
                LOG.error(null, ex);
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void run() {
            assert(m_writer != null);
            assert(m_input != null);
            int location = 0;
            boolean eof = false;
            while (!eof) {
                try {
                    int data = m_input.read();
                    if (data == -1) {
                        eof = true;
                    }
                    else {
                        // look for a sequence of letters matching the server ready token.
                        if (!m_witnessedReady.get() && m_token[location] == data) {
                            location++;
                            if (location == m_token.length) {
                                synchronized (this) {
                                    m_witnessedReady.set(true);
                                    this.notifyAll();
                                }
                            }
                        }
                        else {
                            location = 0;
                        }
                        m_writer.write(data);
                        m_writer.flush();
                    }
                }
                catch (IOException ex) {
                    eof = true;
                }
            }
        }
    }

    public LocalCluster(String prefix, int siteCount,
                        int partitionsPerSite, int replication, BackendTarget target)
    {
        String jarFileName = prefix; // FIXME String.format("%s-")
        System.out.println("Instantiating LocalCluster for " + jarFileName);
        System.out.println("Sites: " + siteCount + " hosts: " + partitionsPerSite
                           + " replication factor: " + replication);

        assert (jarFileName != null);
        assert (siteCount > 0);
        assert (partitionsPerSite > 0);
        assert (replication >= 0);
        
        /*// (1) Load catalog from Jar
        Catalog tmpCatalog = CatalogUtil.loadCatalogFromJar(jarFileName);
        
        // (2) Update catalog to include target cluster configuration
        ClusterConfiguration cc = new ClusterConfiguration();
        // Update cc with a bunch of hosts/sites/partitions
        for (int site = 0, currentPartition = 0; site < hostCount; ++site) {
            for (int partition = 0; partition < siteCount; ++partition, ++currentPartition) {
                cc.addPartition("localhost", site, currentPartition);
            }
        }
        System.err.println(cc.toString());
        this.catalog = FixCatalog.addHostInfo(tmpCatalog, cc);
        
        System.err.println(CatalogInfo.getInfo(this.catalog, new File(jarFileName)));
        System.err.println(catalog.serialize());
        
        // (3) Write updated catalog back out to jar file
        try {
            CatalogUtil.updateCatalogInJar(jarFileName, catalog, "catalog.txt");
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        tmpCatalog = CatalogUtil.loadCatalogFromJar(jarFileName);
        System.err.println("XXXXXXXXXXXXXXXXXXXXX\n" + CatalogInfo.getInfo(this.catalog, new File(jarFileName)));*/
        
        m_jarFileName = new File(VoltServerConfig.getPathToCatalogForTest(jarFileName));
        m_siteCount = siteCount;
        m_partitionPerSite = partitionsPerSite;
        m_target = target;
        m_replication = replication;
        String buildDir = System.getenv("VOLTDB_BUILD_DIR");  // via build.xml
        if (buildDir == null)
            m_buildDir = System.getProperty("user.dir") + "/obj/release";
        else
            m_buildDir = buildDir;

        // processes of VoltDBs using the compiled jar file.
        m_cluster = new ArrayList<Process>();
        m_pipes = new ArrayList<PipeToFile>();
        Thread shutdownThread = new Thread(new ShutDownHookThread());
        java.lang.Runtime.getRuntime().addShutdownHook(shutdownThread);
    }

    @Override
    public boolean compile(VoltProjectBuilder builder) {
        if (m_compiled) {
            LOG.info("ALREADY COMPILED");
            return true;
        }
        m_compiled = builder.compile(m_jarFileName.getAbsolutePath(),
                                     m_partitionPerSite,
                                     m_siteCount,
                                     m_replication,
                                     "localhost");
        
        // (1) Load catalog from Jar
        Catalog tmpCatalog = CatalogUtil.loadCatalogFromJar(m_jarFileName);
        
        // (2) Update catalog to include target cluster configuration
        ClusterConfiguration cc = new ClusterConfiguration();
        // Update cc with a bunch of hosts/sites/partitions
        for (int site = 0, currentPartition = 0; site < m_siteCount; ++site) {
            for (int partition = 0; partition < m_partitionPerSite; ++partition, ++currentPartition) {
                cc.addPartition("localhost", site, currentPartition);
            }
        }
        tmpCatalog = FixCatalog.cloneCatalog(tmpCatalog, cc);
        
        // (3) Write updated catalog back out to jar file
        try {
            CatalogUtil.updateCatalogInJar(m_jarFileName, tmpCatalog);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        // (4) Load it back in as a CatalogContext
        this.catalogContext = CatalogUtil.loadCatalogContextFromJar(m_jarFileName);
        assert(this.catalogContext != null);
        
        // tmpCatalog = CatalogUtil.loadCatalogFromJar(m_jarFileName);
        System.out.println(CatalogInfo.getInfo(this.catalogContext));
        System.out.flush();
        
        return m_compiled;
    }

    @Override
    public void startUp() {
        assert (!m_running);
        if (m_running) {
            LOG.info("ALREADY RUNNING");
            return;
        }
        
        // Construct the base command that we will want to use to start
        // all of the "remote" HStoreSites 
        List<String> siteCommand = new ArrayList<String>();
        CollectionUtil.addAll(siteCommand, 
            "ant",
//            "compile",
            "hstore-site",
            "-Djar=" + m_jarFileName
        );
        // Be sure to include our HStoreConf parameters
        for (Entry<String, String> e : this.confParams.entrySet()) {
            siteCommand.add(String.format("-D%s=%s", e.getKey(), e.getValue()));
        }
        // Lastly, we will include the site.id as the last parameter
        // so that we can easily change it
        siteCommand.add("-Dsite.id=-1");

        LOG.debug("Base command to start remote sites:\n" + StringUtil.join("\n", siteCommand));
        m_procBuilder = new ProcessBuilder(siteCommand.toArray(new String[0]));
        m_procBuilder.redirectErrorStream(true);
        // set the working directory to obj/release/prod
        //m_procBuilder.directory(new File(m_buildDir + File.separator + "prod"));

        // set to true to spew startup timing data
        boolean logtime = true;
        long startTime = 0;
        if (logtime) {
            startTime = System.currentTimeMillis();
            System.out.println("********** Starting cluster at: " + startTime);
        }

        // create the in-process server
//        Configuration config = new Configuration();
//        config.m_backend = m_target;
//        config.m_noLoadLibVOLTDB = (m_target == BackendTarget.HSQLDB_BACKEND);
//        config.m_pathToCatalog = m_jarFileName;
//        config.m_profilingLevel = ProcedureProfiler.Level.DISABLED;
//        config.m_port = HStoreConstants.DEFAULT_PORT;

        HStoreConf hstore_conf = HStoreConf.singleton(HStoreConf.isInitialized() == false);
        hstore_conf.loadFromArgs(this.confParams);
        
        String namePrefix = null;
        if (m_jarFileName.getName().contains("-")) {
            namePrefix = m_jarFileName.getName().split("-")[0].replace(".jar", "");
        }
        
        // create all the out-of-process servers
        // Loop through all of the sites in the catalog and start them
        int offset = m_procBuilder.command().size() - 1;
        for (Site catalog_site : catalogContext.sites) {
            final int site_id = catalog_site.getId(); 
            
            // If this is the first site, then start the HStoreSite in this JVM
            if (site_id == 0) {
                m_localServer = new ServerThread(this.catalogContext, hstore_conf, site_id);
                m_localServer.start();
                if (logtime) {
                    System.out.println("********** Started in-process HStoreSite [siteId=" + site_id + "]");
                    System.out.flush();
                }
            }
            // Otherwise, fork a new JVM that will run our other HStoreSites.
            // Remember that it is one JVM per HStoreSite
            else {
                try {
                    m_procBuilder.command().set(offset, "-Dsite.id=" + site_id);
                    Process proc = m_procBuilder.start();
                    m_cluster.add(proc);
                    // write output to obj/release/testoutput/<test name>-n.txt
                    // this may need to be more unique? Also very useful to just
                    // set this to a hardcoded path and use "tail -f" to debug.
                    String testoutputdir = m_buildDir + File.separator + "testoutput";
                    // make sure the directory exists
                    File dir = new File(testoutputdir);
                    if (dir.exists()) {
                        assert(dir.isDirectory());
                    }
                    else {
                        boolean status = dir.mkdirs();
                        assert(status);
                    }
    
                    String logFile = testoutputdir + File.separator +
                                     (namePrefix != null ? namePrefix + "-" : "") + 
                                     getName() + "-" + site_id + ".txt";
                    PipeToFile ptf = new PipeToFile(logFile, proc.getInputStream());
                    ptf.m_writer.write(m_procBuilder.command().toString() + "\n");
                    m_pipes.add(ptf);
                    Thread t = new Thread(ptf);
                    t.setName("ClusterPipe:" + String.valueOf(site_id));
                    t.start();
                    if (logtime) {
                        System.out.println("********** Started separate HStoreSite process [siteId=" + site_id + "]");
                        System.out.flush();
                    }
                }
                catch (IOException ex) {
                    LOG.fatal("Failed to start cluster process", ex);
                    assert (false);
                }
            }
        }

        // spin until all the pipes see the magic "Server completed.." string.
        boolean allReady;
        do {
            //if (logtime) System.out.println("********** pre witness: " + (System.currentTimeMillis() - startTime) + " ms");
            allReady = true;
            for (PipeToFile pipeToFile : m_pipes) {
                if (pipeToFile.m_witnessedReady.get() != true) {
                    try {
                        // wait for explicit notification
                        synchronized (pipeToFile) {
                            pipeToFile.wait();
                        }
                    }
                    catch (InterruptedException ex) {
                        LOG.error(null, ex);
                    }
                    allReady = false;
                    break;
                }
            }
        } while (allReady == false);
        //if (logtime) System.out.println("********** post witness: " + (System.currentTimeMillis() - startTime) + " ms");

        // Finally, make sure the local server thread is running and wait if it is not.
        m_localServer.waitForInitialization();
        //if (logtime) System.out.println("********** DONE: " + (System.currentTimeMillis() - startTime) + " ms");
        m_running = true;
    }

    @Override
    synchronized public List<String> shutDown() throws InterruptedException {
        // there are couple of ways to shutdown. sysproc @kill could be
        // issued to listener. this would require that the test didn't
        // break the cluster somehow.  Or ... just old fashioned kill?

        try {
            if (m_localServer != null) m_localServer.shutdown();
        } finally {
            m_running = false;
        }
        shutDownExternal();

        return null;
    }

    public void shutDownSingleHost(int hostNum)
    {
        Process proc = m_cluster.get(hostNum);
        proc.destroy();
        m_cluster.remove(hostNum);
    }

    public void shutDownExternal() throws InterruptedException
    {
        if (m_cluster != null) {
            LOG.info("Shutting down cluster");
            for (Process proc : m_cluster) {
                proc.destroy();
                int retval = proc.waitFor();
                // exit code 143 is the forcible shutdown code from .destroy()
                if (retval != 0 && retval != 143)
                {
                    System.out.println("External VoltDB process terminated abnormally with return: " + retval);
                }
            }
        }

        if (m_cluster != null) m_cluster.clear();
    }

    @Override
    public List<String> getListenerAddresses() {
        if (!m_running) {
            return null;
        }
        ArrayList<String> listeners = new ArrayList<String>();
        listeners.add("localhost");
        return listeners;
    }

    @Override
    public String getName() {
        String retval = String.format("localCluster-%d-%d-%s",
                                        m_partitionPerSite,
                                        m_siteCount,
                                        m_target.display.toUpperCase());
        
        if (this.nameSuffix != null && this.nameSuffix.isEmpty() == false)
            retval += "-" + this.nameSuffix;
        
        return retval;
    }

    @Override
    public int getNodeCount()
    {
        return m_siteCount;
    }
    
    @Override
    public int getPartitionCount() {
        return (m_siteCount * m_partitionPerSite);
    }

    @Override
    public CatalogContext getCatalogContext() {
        return this.catalogContext;
    }
    
    @Override
    public Catalog getCatalog() {
        return this.catalogContext.catalog;
    }
    
    @Override
    public void finalize() throws Throwable {
        try {
            shutDownExternal();
        }
        finally {
            super.finalize();
        }
    }

    class ShutDownHookThread implements Runnable {
        @Override
        public void run() {
            try {
                shutDownExternal();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public boolean isHSQL() {
        return m_target == BackendTarget.HSQLDB_BACKEND;
    }

    @Override
    public boolean isValgrind() {
        return m_target == BackendTarget.NATIVE_EE_VALGRIND_IPC;
    }

}
