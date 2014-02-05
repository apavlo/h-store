package org.voltdb.utils;

public class ProcessUtils {
    public static native int fork();
    public static native void kill(int pid);
}
