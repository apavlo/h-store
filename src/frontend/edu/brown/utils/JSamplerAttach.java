/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
/*
 Copyright (c) 2008 Evan Jones

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 THE SOFTWARE.
 */

package edu.brown.utils;

// Useful documentation:
// http://blogs.sun.com/sundararajan/entry/using_mustang_s_attach_api

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.sun.tools.attach.VirtualMachine;

/** Attaches to a running JVM and starts profiling. */
public class JSamplerAttach {
    private static final String CLASSPATH_PROPERTY = "java.class.path";

    /** Splits the classpath into Files. */
    private static File[] classPathParts() {
        String classpath = System.getProperty(CLASSPATH_PROPERTY);
        String separator = System.getProperty("path.separator");

        String[] parts = classpath.split(separator);
        File[] files = new File[parts.length];
        for (int i = 0; i < parts.length; ++i) {
            files[i] = new File(parts[i]);
        }

        return files;
    }

    private static final String JAR_NAME = "jsampler.jar";

    public static void perrorQuit(String message, Exception e) {
        System.err.println("Error: " + message + ": " + e.getMessage());
        System.exit(1);
    }

    public static void main(String[] args) {
        if (args.length != 5) {
            System.err.println("Usage: JSamplerAttach <jvmpid> <duration> <interval> <port> <output file>");
            System.exit(1);
        }

        // Search the classpath for the jar
        File jarPath = null;
        for (File path : classPathParts()) {
            if (path.isDirectory()) {
                File jar = new File(path, JAR_NAME);
                if (jar.canRead()) {
                    jarPath = jar;
                    break;
                }
            } else if (path.getName().equals(JAR_NAME) && path.canRead()) {
                jarPath = path;
                break;
            }
        }

        if (jarPath == null) {
            System.err.println("Error: Could not find " + JAR_NAME + " by searching the classpath");
            System.err.println("CLASSPATH = " + System.getProperty(CLASSPATH_PROPERTY));
            System.err.println("Searched:");
            for (File path : classPathParts()) {
                System.err.println("\t" + path.getAbsolutePath());
            }
            System.exit(1);
        }

        String pid = args[0];
        String duration = args[1];
        String interval = args[2];
        String port = args[3];
        String output = args[4];
        try {
            // Test if we can write to the file by opening it for writing, then
            // deleting it
            FileOutputStream out = new FileOutputStream(output);
            out.close();
            File outFile = new File(output);
            outFile.delete();
            // We need an absolute path to pass to the other JVM
            output = outFile.getAbsolutePath();
        } catch (java.io.FileNotFoundException e) {
            perrorQuit("Cannot write to output file", e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        VirtualMachine vm;
        try {
            vm = VirtualMachine.attach(pid);
        } catch (com.sun.tools.attach.AttachNotSupportedException e) {
            perrorQuit("Attach to pid " + pid + " failed", e);
            throw new RuntimeException("should not get here");
        } catch (IOException e) {
            perrorQuit("Attach to pid " + pid + " failed", e);
            throw new RuntimeException("should not get here");
        }

        try {
            vm.loadAgent(jarPath.getAbsolutePath(), duration + ";" + interval + ";" + port + ";" + output);
        } catch (com.sun.tools.attach.AgentLoadException e) {
            throw new RuntimeException(e);
        } catch (com.sun.tools.attach.AgentInitializationException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
