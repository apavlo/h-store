package edu.brown.api;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.utils.Pair;

/**
 * 
 * @author pavlo
 */
public class BenchmarkClientFileUploader {

    /**
     * Files to send to clients
     * ClientId -> <LocalFile, RemoteFile>
     */
    private final Map<Integer, Map<String, Pair<File, File>>> filesToSend = new HashMap<Integer, Map<String, Pair<File,File>>>();
    
    
    public BenchmarkClientFileUploader() {
        // Nothing to do
    }
    
    /**
     * Queues a file on the local machine to be sent to the host running the client for the given id
     * The local_file is the path on this machine, the remote_file is the path on the remote machine
     * @param client_id
     * @param parameter
     * @param local_file
     * @param remote_file
     * @throws IOException
     */
    public void sendFileToClient(int client_id, String parameter, File local_file, File remote_file) throws IOException {
        if (local_file.exists() == false) {
            throw new IOException("Unable to send local file '" + local_file + "' to client " + client_id + ". File does not exist");
        }
        Map<String, Pair<File, File>> m = this.filesToSend.get(client_id);
        if (m == null) {
            m = new HashMap<String, Pair<File, File>>();
            this.filesToSend.put(client_id, m);
        }
        m.put(parameter, Pair.of(local_file, remote_file));
    }
    
    public Collection<Integer> getClientsWithFiles() {
        return (this.filesToSend.keySet());
    }
    
    public boolean hasFilesToSend() {
        for (Integer id : this.filesToSend.keySet()) {
            if (this.filesToSend.get(id).isEmpty() == false) return (true);
        } // FOR
        return (false);
    }
    
    public boolean hasFilesToSend(int client_id) {
        return (this.filesToSend.containsKey(client_id) && this.filesToSend.get(client_id).isEmpty() == false); 
    }
    
    protected Map<String, Pair<File, File>> getFilesToSend(int client_id) {
        return (this.filesToSend.get(client_id));
    }
    
}
