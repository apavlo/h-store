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

package org.voltdb.processtools;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.brown.utils.CollectionUtil;

public abstract class SSHTools {
    private static final Logger LOG = Logger.getLogger(SSHTools.class);

    private static final List<String> DEFAULT_OPTIONS = new ArrayList<String>();
    static {
        DEFAULT_OPTIONS.add("-q");
        DEFAULT_OPTIONS.add("-C");
        DEFAULT_OPTIONS.add("-o"); DEFAULT_OPTIONS.add("UserKnownHostsFile=/dev/null");
        DEFAULT_OPTIONS.add("-o"); DEFAULT_OPTIONS.add("StrictHostKeyChecking=no");
    }
    
    private static final List<String> SCP_PRUNE_OPTIONS = new ArrayList<String>();
    static {
        SCP_PRUNE_OPTIONS.add("-x");
        SCP_PRUNE_OPTIONS.add("-X");
    }

    public static String createUrl(String user, String hostname, File path) {
        String url = new String();
        if (user != null)
            url = url.concat(user + "@");

        if (hostname != null)
           url = url.concat(hostname);

        if (path != null)
            url = url.concat(":" + path.getPath());

        return url;
    }
    
    /**
     * Delete the given file on the remote host.
     * @param remoteUser
     * @param remoteHost
     * @param remoteFile
     * @param sshOptions
     * @return
     */
    public static boolean deleteFile(String remoteUser, String remoteHost, File remoteFile, String sshOptions[]) {
        String command[] = { "rm", "-f", remoteFile.getPath() };
        String output = ShellTools.cmd(convert(remoteUser, remoteHost, null, sshOptions, command));
        if (output.length() > 1) {
            System.err.print(output);
            return false;
        }
        return true;
    }
    
    /**
     * 
     * @param localPath
     * @param remoteUser
     * @param remoteHost
     * @param remoteFile
     * @param sshOptions
     * @return
     */
    public static boolean copyToRemote(File localPath, String remoteUser, String remoteHost, File remoteFile, String...sshOptions) {
        // scp -q src.getPath remoteUser@hostNameTo:/pathTo
        List<String> command = new ArrayList<String>();
        command.add("scp");
        command.addAll(DEFAULT_OPTIONS);
        CollectionUtil.addAll(command, sshOptions);
        command.add(localPath.getPath());
        command.add(createUrl(remoteUser, remoteHost, remoteFile));
        
        // Remove invalid scp options
        command.removeAll(SCP_PRUNE_OPTIONS);
        
        LOG.debug(String.format("Copying local file '%s' to remote file '%s' on %s",
                               localPath, remoteFile, remoteHost));
        String output = ShellTools.cmd(command);
        if (output.length() > 1) {
            System.err.print(output);
            return false;
        }
        return true;
    }

    @Deprecated
    public static boolean copyFromRemote(String localPath, String remoteUser, String remoteHost, String remotePath, String...sshOptions) {
        return copyFromRemote(new File(localPath),
                              remoteUser,
                              remoteHost,
                              new File(remotePath),
                              sshOptions);
    }
    
    public static boolean copyFromRemote(File localPath, String remoteUser, String remoteHost, File remotePath, String...sshOptions) {
        // scp -q fromhost:path tohost:path
        List<String> command = new ArrayList<String>();
        command.add("scp");
        command.addAll(DEFAULT_OPTIONS);
        CollectionUtil.addAll(command, sshOptions);
        command.add(createUrl(remoteUser, remoteHost, remotePath));
        command.add(localPath.getPath());
        
        // Remove invalid scp options
        command.removeAll(SCP_PRUNE_OPTIONS);

        LOG.debug(String.format("Copying remote file '%s' on %s to local file '%s'",
                               remotePath, remoteHost, localPath));
        String output = ShellTools.cmd(command);
        if (output.length() > 1) {
            System.err.print(output);
            return false;
        }

        return true;
    }

    /**
     * 
     * @param remoteUser
     * @param hostNameFrom
     * @param pathFrom
     * @param hostNameTo
     * @param pathTo
     * @return
     */
    public static boolean copyBetweenRemotes(String remoteUser,
                                             String hostNameFrom, File pathFrom,
                                             String hostNameTo, File pathTo) {
        // scp -q fromhost:path tohost:path
        List<String> command = new ArrayList<String>();
        command.add("scp");
        command.addAll(DEFAULT_OPTIONS);
        command.add(createUrl(remoteUser, hostNameFrom, pathFrom));
        command.add(createUrl(remoteUser, hostNameTo, pathTo));

        // Remove invalid scp options
        command.removeAll(SCP_PRUNE_OPTIONS);
        
        String output = ShellTools.cmd(command);
        if (output.length() > 1) {
            System.err.print(output);
            return false;
        }

        return true;
    }

    public static String cmd(String username, String hostname, String remotePath, String sshOptions[], String command) {
        return ShellTools.cmd(convert(username, hostname, remotePath, sshOptions, command));
    }

    public static String cmd(String username, String hostname, String remotePath, String sshOptions[], String[] command) {
        return ShellTools.cmd(convert(username, hostname, remotePath, sshOptions, command));
    }

    public static String[] convert(String username, String hostname, String remotePath, String sshOptions[], String command) {
        String[] command2 = command.split(" ");
        return convert(username, hostname, remotePath, sshOptions, command2);
    }

    public static String[] convert(String username, String hostname, String remotePath, String sshOptions[], String[] remoteCommand) {
        List<String> l = new ArrayList<String>();
        CollectionUtil.addAll(l, remoteCommand);
        return (convert(username, hostname, remotePath, sshOptions, l));
    }
        
    public static String[] convert(String username, String hostname, String remotePath, String sshOptions[], List<String> remoteCommand) {
        assert(hostname != null);
        
        List<String> command = new ArrayList<String>();
        command.add("ssh");
        command.addAll(DEFAULT_OPTIONS);
        CollectionUtil.addAll(command, sshOptions);
        command.add((username != null ? username + "@" : "") + hostname);
        if (remotePath != null) command.add("cd " + remotePath + ";");
        command.addAll(remoteCommand);

        return command.toArray(new String[0]);
    }

}
