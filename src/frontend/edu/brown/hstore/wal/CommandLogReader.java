package edu.brown.hstore.wal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.voltdb.messaging.FastDeserializer;
import org.voltdb.utils.NotImplementedException;

public class CommandLogReader implements Iterable<LogEntry> {
    
    final FastDeserializer fd;
    final Map<Integer, String> procedures = new HashMap<Integer, String>();
    
    public CommandLogReader(String path) {
        
        FileChannel roChannel = null;
        ByteBuffer readonlybuffer = null;
        File f = new File(path);
        try {
            roChannel = new RandomAccessFile(f, "r").getChannel();
            readonlybuffer = roChannel.map(FileChannel.MapMode.READ_ONLY, 0, (int)roChannel.size());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        assert(readonlybuffer != null);
        this.fd = new FastDeserializer(readonlybuffer);
        
        // TODO: Read in the file header 
    }
    
    @Override
    public Iterator<LogEntry> iterator() {
        Iterator<LogEntry> it = new Iterator<LogEntry>() {

            @Override
            public boolean hasNext() {
                // TODO Auto-generated method stub
                return false;
            }

            @Override
            public LogEntry next() {
                LogEntry entry = null;
                try {
                    entry = fd.readObject(LogEntry.class);
                } catch (IOException ex) {
                    throw new RuntimeException("Failed to deserialize LogEntry!", ex);
                }
                return (entry);
            }

            @Override
            public void remove() {
                throw new NotImplementedException("Can't call remove! You crazy!");
            }
        };
        return (it);
        
        // TODO: We need to figure out how we want to read these entries
        // back in. I suppose we could just make a new LocalTransaction
        // entry each time. What we really should do is recreate
        // the StoredProcedureInvocation and then pass that into
        // the HStoreSite so that we can replay the transaction
        
    }

}
