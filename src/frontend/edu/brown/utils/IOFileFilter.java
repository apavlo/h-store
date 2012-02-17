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
package edu.brown.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.swing.filechooser.FileFilter;

/**
 * @author Andy Pavlo <pavlo@cs.brown.edu>
 */
public class IOFileFilter extends FileFilter {
    // List of extensions
    private final List<String> matches = new ArrayList<String>();

    // Description of the filter
    private final String description;

    // Suffix/Prefix Mode
    private final boolean prefix_mode;

    /**
     * Base Constructor
     * 
     * @param description
     * @param matches
     */
    public IOFileFilter(String description, boolean prefix_mode, Collection<String> matches) {
        this.description = description;
        this.prefix_mode = prefix_mode;
        if (matches != null) {
            this.matches.addAll(matches);
        }
    }

    /**
     * Constructor
     * 
     * @param matches
     *            a list of extensions to add to the filter
     * @param description
     *            the text description of the filter
     */
    public IOFileFilter(String description, Collection<String> matches) {
        this(description, false, matches);
    }

    public IOFileFilter(String description, boolean prefix_mode, String... matches) {
        this(description, prefix_mode, Arrays.asList(matches));
    }

    public IOFileFilter(String description, String... matches) {
        this(description, false, Arrays.asList(matches));
    }

    /**
     * Adds a new extension to the list. We check for dupes
     * 
     * @param ext
     *            a new file extension to add
     */
    public void addExtension(String ext) {
        ext = ext.toLowerCase();
        if (!this.matches.contains(ext)) {
            this.matches.add(ext);
        }
    }

    /**
     * Removes an extension from the internal filter list
     * 
     * @param ext
     *            a file extension to remove
     */
    public void removeExtension(String ext) {
        ext = ext.toLowerCase();
        if (this.matches.contains(ext)) {
            this.matches.remove(ext);
        }
    }

    /**
     * We only want to except files that match
     * 
     * @param f
     *            - the file to check for its extension
     * @return true if the file is acceptable
     */
    @Override
    public boolean accept(File f) {
        if (f.isDirectory()) {
            return true;
        }

        // PREFIX MODE
        if (this.prefix_mode) {
            String name = f.getName();
            for (String prefix : this.matches) {
                if (name.startsWith(prefix)) {
                    return (true);
                }
            } // FOR

            // EXTENSION MODE
        } else {
            String extension = IOFileFilter.getExtension(f);
            if (extension != null) {
                // Check the extension with our list
                return (this.matches.contains(extension.toLowerCase()));
            }
        }
        return false;
    }

    /**
     * getDescription() The description of this filter
     * 
     * @return the string description of the filter
     */
    @Override
    public String getDescription() {
        return (this.description);
    }

    /**
     * getExtension() Returns the file extension of the file
     * 
     * @param f
     *            - the File object to get the extension for
     * @return the string description of the filter
     */
    public static String getExtension(File f) {
        return (IOFileFilter.getExtension(f.getName()));
    }

    /**
     * getExtension() Returns the file extension of a file path
     * 
     * @param s
     *            - the file name to get the extension from
     * @return the string description of the filter
     */
    public static String getExtension(String s) {
        String ext = null;
        int i = s.lastIndexOf('.');

        if ((i > 0) && (i < s.length() - 1)) {
            ext = s.substring(i + 1).toLowerCase();
        }
        return (ext);
    }

    /**
     * Returns the file extension of a file path
     * 
     * @param s
     *            the file name to get the extension from
     * @return the string description of the filter
     */
    public static String getDir(String s) {
        String dir = null;
        int i = s.lastIndexOf('/');

        if ((i > 0) && (i < s.length() - 1)) {
            dir = s.substring(0, i);
        }
        return (dir);
    }
}
