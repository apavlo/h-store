package edu.brown.protorpc;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.SelectableChannel;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;

import edu.brown.net.NIOReadStream;
import edu.brown.net.NonBlockingConnection;

/**
 * Provides a non-blocking stream of protocol buffers. This is fairly low level, and is intended
 * to be a building block for more complex protocols. For example, the ProtoRPC system adds
 * extra logic on top of this "raw" connection. 
 */
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

    /** See {@link NIOReadStream#readAllAvailable()}.
     * @see NIOReadStream#readAllAvailable()
     */
    public boolean readAllAvailable() {
        return  connection.readAllAvailable();
    }

    /** Attempts to read a buffered message from the underlying connection. This is more efficient
     * than attempting to actually read from the underlying connection for each message, when ends
     * up making a final "empty" read from the non-blocking connection, rather than simply
     * consuming all buffered data.
     * 
     * TODO: It would be ideal if there was a way to consume data as we go, instead of buffering
     * it all then consuming it. However, this only matters for streams of medium-sized messages
     * with a huge backlog, which should be rare? The C++ implementation has a similar issue.
     * 
     * @param builder message builder to be parsed
     * @return true if a message was read, false if there is not enough buffered data to read a
     *      message.
     */
    public boolean readBufferedMessage(MessageLite.Builder builder) {
        try {
            if (nextMessageLength == -1) {
                if (connection.available() < 4) {
                    return false;
                }
    
                input.setLimit(4);
                nextMessageLength = codedInput.readRawLittleEndian32();
            }
            assert nextMessageLength >= 0;
    
            if (connection.available() < nextMessageLength) {
                assert 0 <= connection.available() && connection.available() < nextMessageLength;
                return false;
            }
    
            // Parse the response for the next RPC
            // TODO: Add .available() to CodedInputStream to avoid many copies to internal buffer?
            // or make CodedInputStream wrap a non-blocking interface like C++?
            input.setLimit(nextMessageLength);
            builder.mergeFrom(codedInput);
            assert codedInput.isAtEnd();
            codedInput.resetSizeCounter();
            nextMessageLength = -1;
            return true;
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

    // TODO: Only keep one of getConnection and getChannel?
    public NonBlockingConnection getConnection() {
        return connection;
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
