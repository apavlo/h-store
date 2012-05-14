/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import org.apache.log4j.Logger;

public class EELibraryLoader {

    private static boolean voltSharedLibraryLoaded = false;

    private static final Logger hostLog = Logger.getLogger(EELibraryLoader.class);


    static private boolean test64bit() {
        // Sun JVMs are nice and chatty on this topic.
        String sun_arch_data_model = System.getProperty("sun.arch.data.model", "none");
        if (sun_arch_data_model.contains("64")) {
            return true;
        }
        hostLog.info("Unable to positively confirm a 64-bit JVM. VoltDB requires" +
                " a 64-bit JVM. A 32-bit JVM will fail to load the native VoltDB" +
                " library.");
        return false;
    }
    
    public synchronized boolean isExecutionEngineLibraryLoaded() {
        return (voltSharedLibraryLoaded);
    }

    /**
     * Load the shared native library if not yet loaded. Returns true if the library was loaded
     **/
    public synchronized static boolean loadExecutionEngineLibrary(boolean mustSuccede) {
        if (!voltSharedLibraryLoaded) {
            if (VoltDB.getLoadLibVOLTDB()) {
                test64bit();

                try {
                    final String libname = "voltdb"; // -" + VoltDB.instance().getVersionString();
                    hostLog.debug("Attempting to load native VoltDB library " + libname +
                            ". Expect to see a confirmation following this upon success. " +
                            "If none appears then you may need to compile VoltDB for your platform " +
                            "or you may be running a 32-bit JVM.");
                    System.loadLibrary(libname);
                    voltSharedLibraryLoaded = true;
                    hostLog.debug("Successfully loaded native VoltDB library " + libname +
                    ".");
                } catch (Throwable t) {
                    if (mustSuccede) {
                        hostLog.fatal("Library VOLTDB JNI shared library loading failed. Library path "
                                + System.getProperty("java.library.path"), t);
                        VoltDB.crashVoltDB();
                    } else {
                        hostLog.error("Library VOLTDB JNI shared library loading failed. Library path "
                                + System.getProperty("java.library.path"), t);
                    }
                    return false;
                }
            } else {
                return false;
            }
        }
        return voltSharedLibraryLoaded;
    }
}