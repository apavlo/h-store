/**
 * 
 */
package edu.brown.utils;

import java.io.File;

import junit.framework.TestCase;

public class TestFileUtil extends TestCase {

    /**
     * testFindDirectory
     */
    public void testFindDirectory() throws Exception {
        String dirName = "tests";
        File path = FileUtil.findDirectory(dirName);
        assert(path.exists());
        assert(path.isDirectory());
        // System.err.println("Found = " + path);
    }
    
}
