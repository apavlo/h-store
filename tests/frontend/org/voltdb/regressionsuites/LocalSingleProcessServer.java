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
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.ServerThread;
import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.jni.ExecutionEngineIPC;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.conf.HStoreConf;

/**
 * Implementation of a VoltServerConfig for the simplest case:
 * the single-process VoltServer that's so easy to use.
 *
 */
public class LocalSingleProcessServer extends VoltServerConfig {
    private static final Logger LOG = Logger.getLogger(LocalSingleProcessServer.class);

    public final File m_jarFileName;
    public final int m_partitionCount;
    public final BackendTarget m_target;

    CatalogContext catalogContext = null;
    ServerThread m_server = null;
    boolean m_compiled = false;

    public LocalSingleProcessServer(String jarFileName, int partitionCount,
                                    BackendTarget target)
    {
        assert(jarFileName != null);
        assert(partitionCount > 0);
        final String buildType = System.getenv().get("BUILD");
        m_jarFileName = new File(VoltServerConfig.getPathToCatalogForTest(jarFileName));
        m_partitionCount = partitionCount;
        if (buildType == null) {
            m_target = target;
        } else {
            if (buildType.startsWith("memcheck")) {
                if (target.equals(BackendTarget.NATIVE_EE_JNI)) {
                    m_target = BackendTarget.NATIVE_EE_VALGRIND_IPC;
                } else {
                    m_target = target;//For memcheck
                }
            } else {
                m_target = target;
            }
        }
    }

    @Override
    public boolean compile(VoltProjectBuilder builder) {
        if (m_compiled == true) {
            LOG.info("ALREADY COMPILED");
            return true;
        }
        builder.clearPartitions();
        for (int partition = 0; partition < m_partitionCount; ++partition) {
        	builder.addPartition("localhost", 0, partition);
        } // FOR
        m_compiled = builder.compile(m_jarFileName.getAbsolutePath(), m_partitionCount, 0);
        return m_compiled;
    }

    @Override
    public List<String> getListenerAddresses() {
        // return just "localhost"
        if (m_server == null)
            return null;
        ArrayList<String> listeners = new ArrayList<String>();
        listeners.add("localhost");
        return listeners;
    }

    @Override
    public String getName() {
        // name is combo of the classname and the parameters

        String retval = "localSingleProcess-";
        retval += String.valueOf(m_partitionCount);
        if (m_target == BackendTarget.HSQLDB_BACKEND)
            retval += "-HSQL";
        else if (m_target == BackendTarget.NATIVE_EE_IPC)
            retval += "-IPC";
        else
            retval += "-JNI";
        
        if (this.nameSuffix != null && this.nameSuffix.isEmpty() == false)
            retval += "-" + this.nameSuffix;
        
        return retval;
    }

    @Override
    public int getNodeCount()
    {
        return 1;
    }
    
    @Override
    public int getPartitionCount() {
        return (m_partitionCount);
    }

    @Override
    public List<String> shutDown() throws InterruptedException {
        m_server.shutdown();
        if (m_target == BackendTarget.NATIVE_EE_VALGRIND_IPC) {
            if (!ExecutionEngineIPC.m_valgrindErrors.isEmpty()) {
                ArrayList<String> retval = new ArrayList<String>(ExecutionEngineIPC.m_valgrindErrors);
                ExecutionEngineIPC.m_valgrindErrors.clear();
                return retval;
            }
        }
        return null;
    }

    @Override
    public void startUp() {
//        Configuration config = new Configuration();
//        config.m_backend = m_target;
//        config.m_noLoadLibVOLTDB = (m_target == BackendTarget.HSQLDB_BACKEND);
//        // m_jarFileName is already prefixed with test output path.
//        config.m_pathToCatalog = m_jarFileName;
//        config.m_profilingLevel = ProcedureProfiler.Level.DISABLED;

        this.catalogContext = CatalogUtil.loadCatalogContextFromJar(m_jarFileName);
        HStoreConf hstore_conf = HStoreConf.singleton(HStoreConf.isInitialized() == false);
        hstore_conf.loadFromArgs(this.confParams);
        m_server = new ServerThread(this.catalogContext, hstore_conf, 0);
        m_server.start();
        m_server.waitForInitialization();
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
    public boolean isHSQL() {
        return m_target == BackendTarget.HSQLDB_BACKEND;
    }

    @Override
    public boolean isValgrind() {
        return m_target == BackendTarget.NATIVE_EE_VALGRIND_IPC;
    }
}
