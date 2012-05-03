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

package org.voltdb.exceptions;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.voltdb.VoltProcedure;

/**
 * Base class for runtime exceptions that can be serialized to ByteBuffers without involving Java's
 * serialization mechanism
 *
 */
public class SerializableException extends VoltProcedure.VoltAbortException {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Storage for the detailed message describing the error that generated this exception
     */
    private final String m_message;

    /**
     * Enum correlating the integer ordinals that are serialized as the type of an exception
     * with the class that deserializes that type.
     *
     */
    protected enum SerializableExceptions {
        None() {
            @Override
            protected SerializableException deserializeException(ByteBuffer b) {
                return null;
            }
        },
        EEException() {
            @Override
            protected SerializableException deserializeException(ByteBuffer b) {
                return new EEException(b);
            }
        },
        SQLException() {
            @Override
            protected SerializableException deserializeException(ByteBuffer b) {
                return new SQLException(b);
            }
        },
        ConstraintFailureException() {
            @Override
            protected SerializableException deserializeException(ByteBuffer b) {
                return new ConstraintFailureException(b);
            }
        },
        MispredictionException() {
            @Override
            protected SerializableException deserializeException(ByteBuffer b) {
                return new MispredictionException(b);
            }
        },
        GenericSerializableException() {
            @Override
            protected SerializableException deserializeException(ByteBuffer b) {
                return new SerializableException(b);
            }
        };

        abstract protected SerializableException deserializeException(ByteBuffer b);
    }

    public SerializableException() {
        m_message = null;
    }

    public SerializableException(Throwable t) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        pw.flush();
        m_message = sw.toString();
    }
    
    public SerializableException(ByteBuffer b) {
        final int messageLength = b.getInt();
        final byte messageBytes[] = new byte[messageLength];
        b.get(messageBytes);
        try {
            m_message = new String(messageBytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the detailed message describing the error that generated this exception
     */
    @Override
    public String getMessage() { return m_message; }

    /**
     * Number of bytes necessary to store the serialized representation of this exception
     * @return Number of bytes
     */
    public int getSerializedSize() {
        // sizes:  as near as I can tell,
        // 3 is sizeof(short) for buffer length and sizeof(byte) for exception type
        // 4 is sizeof(int) for message string length
        if (m_message == null) {
            return 5 + 4 + p_getSerializedSize();
        }
        return 5 + 4 + m_message.getBytes().length + p_getSerializedSize();//one byte ordinal and 4 byte length
    }

    /**
     * Method for subclasses to implement that returns the number of bytes necessary to store
     * subclass data
     * @return Number of bytes necessary to store subclass data
     */
    protected int p_getSerializedSize() {
        return 0;
    }

    /**
     * Serialize this exception to the supplied byte buffer
     * @param b ByteBuffer to serialize this exception to
     * @throws IOException
     */
    public void serializeToBuffer(ByteBuffer b) throws IOException {
        assert(getSerializedSize() <= b.remaining());
        b.putInt(getSerializedSize() - 4);
        b.put((byte)getExceptionType().ordinal());
        if (m_message != null) {
            final byte messageBytes[] = m_message.getBytes();
            b.putInt(messageBytes.length);
            b.put(messageBytes);
        } else {
            b.putInt(0);
        }
        p_serializeToBuffer(b);
    }
    
    /**
     * Serialize this exception to a new ByteBuffer
     * WARNING: This is slow and should normally be used!
     * @return
     */
    public ByteBuffer serializeToBuffer() {
        int size = this.getSerializedSize();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        try {
            this.serializeToBuffer(buffer);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return (buffer);
    }

    /**
     * Method for subclasses to implement that serializes the subclass's contents to
     * the ByteBuffer
     * @param b ByteBuffer to serialize the subclass contents to
     * @throws IOException
     */
    protected void p_serializeToBuffer(ByteBuffer b) throws IOException {}

    /**
     * Method for subclasses to specify what constant from the SerializableExceptions enum
     * is defined for this type of exception
     * @return Type of excption
     */
    protected SerializableExceptions getExceptionType() {
        return SerializableExceptions.GenericSerializableException;
    }

    /**
     * Deserialize an exception (if any) from the ByteBuffer
     * @param b ByteBuffer containing the exception to be deserialized
     * @return A deserialized exception if one was serialized or null
     */
    public static SerializableException deserializeFromBuffer(ByteBuffer b) {
        final int length = b.getInt();
        if (length == 0) {
            return null;
        }
        final int ordinal = b.get();
        assert (ordinal != SerializableExceptions.None.ordinal());
        return SerializableExceptions.values()[ordinal].deserializeException(b);
    }

    @Override
    public void printStackTrace(PrintStream s) {
        s.print(m_message);
    }

    @Override
    public void printStackTrace(PrintWriter p) {
        p.print(m_message);
    }
}
