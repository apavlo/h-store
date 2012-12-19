package org.voltdb.exceptions;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * 
 * @author pavlo
 */
public class ServerFaultException extends SerializableException {
    private static final long serialVersionUID = 1L;
    
    private final String errorMessage;
    
    /**
     * The transaction id that caused this exception
     */
    private Long txn_id;

    /**
     * Constructor
     * @param txn_id
     */
    public ServerFaultException(String message, Long txn_id) {
        super(new Exception(message));
        this.errorMessage = message;
        this.txn_id = txn_id;
    }
    
    public ServerFaultException(String message, Throwable t) {
        this(message, t, null);
    }
    
    public ServerFaultException(String message, Throwable t, Long txn_id) {
        super(t);
        this.errorMessage = message;
        this.txn_id = txn_id;
    }
    
    /**
     * Constructor for deserializing an exception from a ByteBuffer
     * @param exceptionBuffer
     */
    public ServerFaultException(ByteBuffer exceptionBuffer) {
        super(exceptionBuffer);
        this.txn_id = exceptionBuffer.getLong();
        
        int errorMessageLength = exceptionBuffer.getInt();
        final byte errorMessageBytes[] = new byte[errorMessageLength];
        exceptionBuffer.get(errorMessageBytes);
        try {
            this.errorMessage = new String(errorMessageBytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * The transaction id that caused this exception
     * @return
     */
    public long getTransactionId() {
        return this.txn_id;
    }
    
    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("Server Fault");
        if (this.txn_id != null)
            sb.append(" in txn #" + this.txn_id);
        sb.append(" - " + this.errorMessage);
        if (this.getCause() != null)
            sb.append("\nCaused By " + this.getCause().getMessage());
        return (sb.toString());
            
    }
    
}
