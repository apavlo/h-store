/***************************************************************************
 *   Copyright (C) 2008 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.utils;

import java.io.*;

import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * @author pavlo
 */
public abstract class FileUtil {
    private static final Logger LOG = Logger.getLogger(FileUtil.class.getName());

    public static void writeStringToFile(String file_path, String content) throws Exception {
        FileUtil.writeStringToFile(new File(file_path), content);
    }

    public static void writeStringToFile(File file, String content) throws Exception {
        FileWriter writer = new FileWriter(file);
        writer.write(content);
        writer.flush();
        writer.close();
    }
    
    public static File writeStringToTempFile(String content) {
        File tempFile;
        try {
            tempFile = File.createTempFile("myApp", ".tmp");
            tempFile.deleteOnExit();
            
            FileUtil.writeStringToFile(tempFile, content);
            
            return tempFile;
            
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    
    public static String readFile(File path) throws IOException {
        return (readFile(path.getAbsolutePath()));
    }
    
    public static String readFile(String path) throws IOException {
        StringBuilder buffer = new StringBuilder();
        BufferedReader in = FileUtil.getReader(path);
        while (in.ready()) {
            buffer.append(in.readLine()).append("\n");
        } // WHILE
        in.close();
        return (buffer.toString());
    }
    
    /**
     * Creates a BufferedReader for the given input path
     * Can handle both gzip and plain text files
     * @param path
     * @return
     * @throws IOException
     */
    public static BufferedReader getReader(String path) throws IOException {
        File file = new File(path);
        if (!file.exists()) {
            throw new IOException("ERROR: The file '" + path + "' does not exist");
        }
        
        BufferedReader in = null;
        if (file.getPath().endsWith(".gz")) {
            FileInputStream fin = new FileInputStream(file);
            GZIPInputStream gzis = new GZIPInputStream(fin);
            in = new BufferedReader(new InputStreamReader(gzis));
            LOG.debug("Reading in the zipped contents of '" + file.getName() + "'");
        } else {
            in = new BufferedReader(new FileReader(file));
            LOG.debug("Reading in the contents of '" + file.getName() + "'");
        }
        return (in);
    }
    
    public static byte[] readBytesFromFile(String path) throws IOException {
        File file = new File(path);
        FileInputStream in = new FileInputStream(file);

        // Create the byte array to hold the data
        long length = file.length();
        byte[] bytes = new byte[(int)length];

        LOG.debug("Reading in the contents of '" + file.getAbsolutePath() + "'");
        
        // Read in the bytes
        int offset = 0;
        int numRead = 0;
        while ( (offset < bytes.length) && ( (numRead=in.read(bytes, offset, bytes.length-offset)) >= 0) ) {
            offset += numRead;
        } // WHILE
        if (offset < bytes.length) {
            throw new IOException("Failed to read the entire contents of '" + file.getName() + "'");
        }
        in.close();
        return (bytes);
    }
    
    /**
     * Find the path to a directory below our current location in the source tree
     * Throws a RuntimeException if we go beyond our repository checkout
     * @param dirName
     * @return
     * @throws IOException
     */
    public static File findDirectory(String dirName) throws IOException {
        return (FileUtil.findDirectory(dirName, new File(".").getCanonicalFile()).getCanonicalFile());
    }
    
    private static final File findDirectory(String dirName, File current) throws IOException {
        LOG.debug("FindDirectory Current Location = " + current);
        boolean has_svn = false;
        for (File file : current.listFiles()) {
            if (file.getCanonicalPath().endsWith(File.separator + dirName)) {
                return (file);
            // Make sure that we don't go to far down...
            } else if (file.getCanonicalPath().endsWith(File.separator + ".svn")) {
                has_svn = true;
            }
        } // FOR
        // If we didn't see an .svn directory, then we went too far down
        if (!has_svn)
            throw new RuntimeException("Unable to find directory '" + dirName + "' [last_dir=" + current.getAbsolutePath() + "]");  
        File next = new File(current.getCanonicalPath() + File.separator + "..");
        return (FileUtil.findDirectory(dirName, next));
    }
    
    /**
     * Returns a list of all the files in a directory whose name starts with the provided prefix
     * @param dir
     * @param filePrefix
     * @return
     * @throws IOException
     */
    public static List<File> getFilesInDirectory(final File dir, final String filePrefix) throws IOException {
        assert(dir.isDirectory()) : "Invalid search directory path: " + dir;
        FilenameFilter filter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return (name.startsWith(filePrefix));
            }
        };
        return (Arrays.asList(dir.listFiles(filter)));
    }
}
