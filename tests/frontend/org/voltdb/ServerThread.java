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

package org.voltdb;

import org.voltdb.catalog.Catalog;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConf;

/**
 * Wraps VoltDB in a Thread
 */
public class ServerThread extends Thread {
    final VoltDB.Configuration m_config;
    final HStoreConf hstore_conf;
    boolean initialized = false;

    // TODO(mainak) Pass this in as an argument to the constructor
    // final Site catalog_site;

    public ServerThread(VoltDB.Configuration config) {
        m_config = config;
        setName("ServerThread");
        
        // Use the default HStoreConf
        // TODO(mainak) Pass this as an argument to the constructor
        this.hstore_conf = HStoreConf.singleton();

        // Load the catalog
        // TODO(mainak) Move out of here
        // this.catalog = CatalogUtil.loadCatalogFromJar(m_config.m_pathToCatalog);
    }

    public ServerThread(String jarfile, BackendTarget target) {
        m_config = new VoltDB.Configuration();
        m_config.m_pathToCatalog = jarfile;
        m_config.m_backend = target;
        
        // Use the default HStoreConf
        // TODO(mainak) Pass this as an argument to the constructor
        this.hstore_conf = HStoreConf.singleton();
        
        // Load the catalog
        // TODO(mainak) Move out of here
        // this.catalog = CatalogUtil.loadCatalogFromJar(m_config.m_pathToCatalog);
    }

    @Override
    public void run() {
        VoltDB.initialize(m_config);
        VoltDB.instance().run();
        
        // FIXME(mainak) HStore.initialize(catalog_site, hstore_conf);
        // FIXME(mainak) HStore.instance().run();
    }

    public void waitForInitialization() {
        // Wait until the server has actually started running.
        while (!VoltDB.instance().isRunning()) {
        // FIXME(mainak) while (!HStore.instance().isRunning()) {
            Thread.yield();
        }
    }

    public void shutdown() throws InterruptedException {
        assert Thread.currentThread() != this;
        VoltDB.instance().shutdown(this);
        // FIXME(mainak) HStore.instance().shutdown();
        this.join();
    }
}
