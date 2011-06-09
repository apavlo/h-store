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

import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;

public abstract class SSHTools {

    private static final List<String> DEFAULT_OPTIONS = new ArrayList<String>();
    static {
        DEFAULT_OPTIONS.add("-q");
        DEFAULT_OPTIONS.add("-o"); DEFAULT_OPTIONS.add("UserKnownHostsFile=/dev/null");
        DEFAULT_OPTIONS.add("-o"); DEFAULT_OPTIONS.add("StrictHostKeyChecking=no");
    }
    

    public static String createUrl(String user, String hostname, String path) {
        String url = new String();
        if (user != null)
            url = url.concat(user + "@");

        if (hostname != null)
           url = url.concat(hostname);

        if (path != null)
            url = url.concat(":" + path);

        return url;
    }
    
    public static boolean writeFile(String contents, String remoteUser, String hostNameTo, String pathTo, String sshOptions[]) {
        File f = FileUtil.writeStringToTempFile(contents, "dtxn.conf", true);
        return (copyToRemote(f, remoteUser, hostNameTo, pathTo));
    }

    public static boolean copyToRemote(File src, String remoteUser, String hostNameTo, String pathTo, String...sshOptions) {
        // scp -q src.getPath remoteUser@hostNameTo:/pathTo
        List<String> command = new ArrayList<String>();
        command.add("scp");
        command.addAll(DEFAULT_OPTIONS);
        CollectionUtil.addAll(command, sshOptions);
        command.add(src.getPath());
        command.add(createUrl(remoteUser, hostNameTo, pathTo));
        String output = ShellTools.cmd(command);
        if (output.length() > 1) {
            System.err.print(output);
            return false;
        }
        return true;
    }

    public static boolean copyFromRemote(File dst, String remoteUser, String hostNameFrom, String pathFrom, String...sshOptions) {
        // scp -q fromhost:path tohost:path
        List<String> command = new ArrayList<String>();
        command.add("scp");
        command.addAll(DEFAULT_OPTIONS);
        CollectionUtil.addAll(command, sshOptions);
        command.add(createUrl(remoteUser, hostNameFrom, pathFrom));
        command.add(dst.getPath());

        String output = ShellTools.cmd(command);
        if (output.length() > 1) {
            System.err.print(output);
            return false;
        }

        return true;
    }

    public static boolean copyBetweenRemotes(String remoteUser, String hostNameFrom, String pathFrom,
            String hostNameTo, String pathTo) {
        // scp -q fromhost:path tohost:path
        List<String> command = new ArrayList<String>();
        command.add("scp");
        command.addAll(DEFAULT_OPTIONS);
        command.add(createUrl(remoteUser, hostNameFrom, pathFrom));
        command.add(createUrl(remoteUser, hostNameTo, pathTo));

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

    public static String[] convert(String username, String hostname, String remotePath, String sshOptions[], String[] command) {
        assert(hostname != null);
        int sshArgCount = 7 + (remotePath == null ? 0 : 1) + sshOptions.length;

        int i = 0;
        String[] retval = new String[command.length + sshArgCount];
        retval[i++] = "ssh";
        retval[i++] = "-q";
        retval[i++] = "-o";
        retval[i++] = "UserKnownHostsFile=/dev/null";
        retval[i++] = "-o";
        retval[i++] = "StrictHostKeyChecking=no";
        for (String opt : sshOptions) {
            retval[i++] = opt;
        }
        
        retval[i] = "";
        if (username != null)
            retval[i] = retval[i].concat(username + "@");
        retval[i] = retval[i].concat(hostname);
        i++;
        
        if (remotePath != null)
            retval[i++] = "cd " + remotePath + ";";
        for (int j = 0; j < command.length; j++) {
            assert(command[j] != null);
            retval[i + j] = command[j];
        }

        return retval ;
    }

    public static void main(String[] args) {
        System.out.print(cmd(null, "volt3b", null, new String[0], "echo foo"));
        System.out.println(copyToRemote(new File("build.py"), null, "volt3b", "."));
    }
}
