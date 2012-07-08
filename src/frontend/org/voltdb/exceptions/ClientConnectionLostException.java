package org.voltdb.exceptions;

public class ClientConnectionLostException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final Long txn_id;
    
    public ClientConnectionLostException(Long txn_id) {
        super();
        this.txn_id = txn_id;
    }
    
    public ClientConnectionLostException(Long txn_id, Throwable cause) {
        super(cause);
        this.txn_id = txn_id;
    }
    
    public Long getTransactionId() {
        return (this.txn_id);
    }
    
}
