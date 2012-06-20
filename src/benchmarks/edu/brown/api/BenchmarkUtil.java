package edu.brown.api;

public abstract class BenchmarkUtil {

    public static String getClientName(String host, int id) {
        return String.format("%s-%03d", host, id);
    }
    
}
