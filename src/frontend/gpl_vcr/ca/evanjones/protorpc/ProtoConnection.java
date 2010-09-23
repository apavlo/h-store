package ca.evanjones.protorpc;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.SelectableChannel;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;

import edu.mit.net.NonBlockingConnection;

/** Provides a non-blocking stream of protocol buffer messages. */ 
public class ProtoConnection {
    private final NonBlockingConnection connection;
    private final NonBlockingConnection.NonBlockingConnectionInputStream input;
    private final OutputStream output;
    private final CodedInputStream codedInput;
    private final CodedOutputStream codedOutput;
    private int nextMessageLength = -1;

    public ProtoConnection(NonBlockingConnection connection) {
        this.connection = connection;
        input = connection.getInputStream();
        output = connection.getOutputStream();
        codedInput = CodedInputStream.newInstance(input);
        codedOutput = CodedOutputStream.newInstance(output);
    }

    public enum Status {
        NO_MESSAGE,
        MESSAGE,
        CLOSED,
    }

    public Status tryRead(MessageLite.Builder builder) {
        try {
            if (nextMessageLength == -1) {
                int available = connection.readAvailable(4);
                if (available < 4) {
                    if (available == -1) return Status.CLOSED;
                    assert 0 <= available && available < 4;
                    return Status.NO_MESSAGE;
                }
    
                input.setLimit(4);
                nextMessageLength = codedInput.readRawLittleEndian32();
            }
            assert nextMessageLength >= 0;
    
            int available = connection.readAvailable(nextMessageLength);
            if (available < nextMessageLength) {
                if (available == -1) return Status.CLOSED;
                assert 0 <= available && available < nextMessageLength;
                return Status.NO_MESSAGE;
            }
    
            // Parse the response for the next RPC
            input.setLimit(nextMessageLength);
            builder.mergeFrom(codedInput);
            assert codedInput.isAtEnd();
            codedInput.resetSizeCounter();
            nextMessageLength = -1;
            return Status.MESSAGE;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean tryWrite(MessageLite message) {
        try {
            codedOutput.writeRawLittleEndian32(message.getSerializedSize());
            message.writeTo(codedOutput);
            // writes to the underlying output stream 
            codedOutput.flush();  

            return connection.tryFlush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public SelectableChannel getChannel() {
        return connection.getChannel();
    }

    public boolean writeAvailable() {
        return connection.writeAvailable();
    }

    public void close() {
        connection.close();
    }
}
