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

import java.io.File;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStore;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.ThreadUtil;

/**
 * Wraps VoltDB in a Thread
 */
public class ServerThread extends Thread {
    
    final CatalogContext catalogContext;
    final HStoreConf hstore_conf;
    final int site_id;
    boolean initialized = false;
    HStoreSite hstore_site;
    
    public ServerThread(CatalogContext catalogContext, HStoreConf hstore_conf, int site_id) {
        setName("ServerThread");
        this.catalogContext = catalogContext;
        this.hstore_conf = hstore_conf;
        this.site_id = site_id;
    }

    @Deprecated
    public ServerThread(String jarfile, BackendTarget target) {
        this.hstore_conf = HStoreConf.singleton();
        this.catalogContext = CatalogUtil.loadCatalogContextFromJar(new File(jarfile));
        this.site_id = -1;
    }

    @Override
    public void run() {
        this.hstore_site = HStore.initialize(this.catalogContext, this.site_id, this.hstore_conf);
        this.hstore_site.run();
    }

    public void waitForInitialization() {
        // Wait until the server has actually started running.
        int counter = 100;
        while (this.hstore_site == null || this.hstore_site.isRunning() == false) {
            ThreadUtil.sleep(100);
            if (counter-- == 0) {
                break;
            }
        } // WHILE
        if (counter == 0) {
            throw new RuntimeException("Failed to start server thread!");
        }
    }

    public void shutdown() throws InterruptedException {
        assert Thread.currentThread() != this;
        this.hstore_site.shutdown();
        this.join();
    }
}
