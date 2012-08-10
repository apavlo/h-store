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

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.voltdb.processtools.SSHTools;
import org.voltdb.processtools.ShellTools;

import edu.brown.hstore.conf.HStoreConf;

public class KillStragglers implements Runnable {
    private static final Logger LOG = Logger.getLogger(KillStragglers.class);

    final String m_username;
    final String m_hostname;
    final String m_remotePath;
    final String m_sshOptions[];
    
    // Options
    Integer m_siteid = null;
    boolean m_killSite = false;
    boolean m_killCoordinator = false;
    boolean m_killEngine = false;
    boolean m_killClient = false;
    
    
    public KillStragglers(String username, String hostname, String remotePath, String sshOptions[]) {
        m_username = username;
        m_hostname = hostname;
        m_remotePath = remotePath;
        m_sshOptions = sshOptions;
    }
    
    public void setSiteId(int siteid) {
         m_siteid = siteid;
    }
    
    public KillStragglers enableKillAll() {
        return (this.enableKillClient().enableKillCoordinator().enableKillSite().enableKillEngine());
    }
    public KillStragglers enableKillSite() {
        m_killSite = true;
        return (this);
    }
    public KillStragglers enableKillCoordinator() {
        m_killCoordinator = true;
        return (this);
    }
    public KillStragglers enableKillEngine() {
        m_killEngine = true;
        return (this);
    }
    public KillStragglers enableKillClient() {
        m_killClient = true;
        return (this);
    }

    @Override
    public void run() {
        assert(m_killSite || m_killCoordinator || m_killEngine || m_killClient) : "No kill option selected";
        
        // Kill Command
        String kill_cmd = "tools/killstragglers.py";
        if (m_killSite) {
            kill_cmd += " --hstoresite";
            if (m_siteid != null) kill_cmd += " --siteid=" + m_siteid;
        }
        if (m_killCoordinator) kill_cmd += " --protocoord";
        if (m_killEngine) kill_cmd += " --protoengine";
        if (m_killClient) kill_cmd += " --client";
        if (LOG.isDebugEnabled()) {
            kill_cmd += " --debug=" + HStoreConf.singleton().global.log_dir;
        }
        
        String cmd[] = SSHTools.convert(m_username, m_hostname, m_remotePath, m_sshOptions, kill_cmd); 
        LOG.debug("KILL PUSSY CAT KILL: " + Arrays.toString(cmd));
        ShellTools.cmdToStdOut(cmd);
        LOG.debug("Finished killing at " + m_hostname);
    }
}
