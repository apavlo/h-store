/* This file is part of VoltDB.
 * Copyright (C) 2008-2009 VoltDB L.L.C.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.*;
import java.util.Date;

public class Bench {
    private static native void foo();
    private native void bar(long engine, boolean undo, boolean rwset, long fragid, byte[] paramset);
    private native void bar2(long engine, long fragid, ByteBuffer paramset, int paramlength, ByteBuffer result, int resultcapacity);
    private native void bar3(long engine, long fragid, ByteBuffer paramset, int paramlength, ByteBuffer result, int resultcapacity);
    private native byte[] bar3a(long engine, long fragid, byte[] paramset);
    private native void bar4(long engine, long fragid, ByteBuffer paramset, int paramlength, ByteBuffer result, int resultcapacity);
    public static void main(String[] args) {
        for (int trial = 0; trial < 5; ++trial) { // to ignore class loading and JIT cost
        {
            System.out.println("foo()");
            int count = 1000000;
            long start = System.nanoTime();
            for (int i = 0; i < count; i++)
                foo();
            long end = System.nanoTime();
            System.out.println(end - start + " ns for " + count + " calls");
            System.out.println((((double) end - start) / count) + " ns/call");
        }

        {
            System.out.println("bar()");
            int count = 1000000;
            Bench bench = new Bench();
            byte[] array = new byte[] {};
            long start = System.nanoTime();
            for (int i = 0; i < count; i++)
                bench.bar(0L, false, false, 0L, array);
            long end = System.nanoTime();
            System.out.println(end - start + " ns for " + count + " calls");
            System.out.println((((double) end - start) / count) + " ns/call");
        }
        {
            System.out.println("bar2()");
            int count = 1000000;
            Bench bench = new Bench();
            ByteBuffer params = ByteBuffer.allocateDirect(500);
            byte[] org = "kljajrlgkjadlkjfglkajdflkgjadlkjgkljdfklgjdfajalksjdlkajskldjlajsdkjaklsjdljaskdjaksjdkajlsd".getBytes();
            byte[] buf2 = new byte[org.length];
            ByteBuffer result = ByteBuffer.allocateDirect(1000000);
            long start = System.nanoTime();
            for (int i = 0; i < count; i++) {
                params.clear();
                params.put(org);
                result.clear();
                bench.bar2(0L, 0L, params, org.length, result, result.capacity());
                result.get(buf2);
            }
            long end = System.nanoTime();
            System.out.println(end - start + " ns for " + count + " calls");
            System.out.println((((double) end - start) / count) + " ns/call");
        }
        {
            System.out.println("bar3()");
            int count = 1000000;
            Bench bench = new Bench();
            ByteBuffer params = ByteBuffer.allocateDirect(500);
            byte[] org = "kljajrlgkjadlkjfglkajdflkgjadlkjgkljdfklgjdfajalksjdlkajskldjlajsdkjaklsjdljaskdjaksjdkajlsd".getBytes();
            byte[] buf2 = new byte[org.length];
            ByteBuffer result = ByteBuffer.allocateDirect(1000000);
            long start = System.nanoTime();
            for (int i = 0; i < count / 5; i++) {
                params.clear();
                params.put(org);
                params.put(org);
                params.put(org);
                params.put(org);
                params.put(org);
                result.clear();
                bench.bar3(0L, 0L, params, org.length, result, result.capacity());
                result.get(buf2);
                result.get(buf2);
                result.get(buf2);
                result.get(buf2);
                result.get(buf2);
            }
            long end = System.nanoTime();
            System.out.println(end - start + " ns for " + count + " calls");
            System.out.println((((double) end - start) / count) + " ns/call");
        }
        {
            System.out.println("bar3a()");
            int count = 1000000;
            Bench bench = new Bench();
            byte[] org = "kljajrlgkjadlkjfglkajdflkgjadlkjgkljdfklgjdfajalksjdlkajskldjlajsdkjaklsjdljaskdjaksjdkajlsd".getBytes();
            long start = System.nanoTime();
            for (int i = 0; i < count; i++) {
                byte[] params = (byte[]) org.clone();
                byte[] buf2 = bench.bar3a(0L, 0L, params);
            }
            long end = System.nanoTime();
            System.out.println(end - start + " ns for " + count + " calls");
            System.out.println((((double) end - start) / count) + " ns/call");
        }
        {
            System.out.println("bar4()");
            int count = 1000000;
            Bench bench = new Bench();
            ByteBuffer params = ByteBuffer.allocateDirect(500);
            byte[] org = "kljajrlgkjadlkjfglkajdflkgjadlkjgkljdfklgjdfajalksjdlkajskldjlajsdkjaklsjdljaskdjaksjdkajlsd".getBytes();
            byte[] buf2 = new byte[org.length];
            ByteBuffer result = ByteBuffer.allocateDirect(1000000);
            params.clear();
            params.put(org);
            result.clear();
            long start = System.nanoTime();
            for (int i = 0; i < count; i++) {
                bench.bar4(0L, 0L, params, org.length, result, result.capacity());
            }
            long end = System.nanoTime();
            System.out.println(end - start + " ns for " + count + " calls");
            System.out.println((((double) end - start) / count) + " ns/call");
        }
        {
            System.out.println("10params_direct()");
            int count = 1000000;
            Bench bench = new Bench();
            ByteBuffer direct = ByteBuffer.allocateDirect(4096);
            Object[] params = new Object[]{1L, 2.0d, "asdjhhjh", 4L, 5L, 6.0d, "fgkljdfgljkdfg", 8L, 9L, 10.0d};
            long start = System.nanoTime();
            for (int i = 0; i < count; i++) {
                direct.clear();
                writeParams(params, direct);
            }
            long end = System.nanoTime();
            System.out.println(end - start + " ns for " + count + " calls");
            System.out.println((((double) end - start) / count) + " ns/call");
        }
        {
            System.out.println("10params_doublebuffer()");
            int count = 1000000;
            Bench bench = new Bench();
            ByteBuffer direct = ByteBuffer.allocateDirect(4096);
            byte[] heap_bytes = new byte[4096];
            ByteBuffer tmp = ByteBuffer.wrap(heap_bytes);
            Object[] params = new Object[]{1L, 2.0d, "asdjhhjh", 4L, 5L, 6.0d, "fgkljdfgljkdfg", 8L, 9L, 10.0d};
            long start = System.nanoTime();
            for (int i = 0; i < count; i++) {
                direct.clear();
                tmp.clear();
                writeParams(params, tmp);
                direct.put(heap_bytes, 0, tmp.position());
            }
            long end = System.nanoTime();
            System.out.println(end - start + " ns for " + count + " calls");
            System.out.println((((double) end - start) / count) + " ns/call");
        }
        {
            System.out.println("10params_myown()");
            int count = 1000000;
            Bench bench = new Bench();
            ByteBuffer direct = ByteBuffer.allocateDirect(4096);
            byte[] heap_bytes = new byte[4096];
            MyLEHeapBuffer tmp = new MyLEHeapBuffer(heap_bytes);
            Object[] params = new Object[]{1L, 2.0d, "asdjhhjh", 4L, 5L, 6.0d, "fgkljdfgljkdfg", 8L, 9L, 10.0d};
            long start = System.nanoTime();
            for (int i = 0; i < count; i++) {
                direct.clear();
                tmp.clear();
                writeParams(params, tmp);
                direct.put(heap_bytes, 0, tmp.position());
            }
            long end = System.nanoTime();
            System.out.println(end - start + " ns for " + count + " calls");
            System.out.println((((double) end - start) / count) + " ns/call");
        }
        }
    }
    static {
        System.loadLibrary("Bench");
    }
    private static void writeParams(Object[] params, ByteBuffer buffer) {
        buffer.put((byte) params.length);
        
        for (Object obj : params) {
            if (obj == null) {
                VoltType type = VoltType.NULL;
                buffer.put(type.getValue());
                continue;
            }

            Class<?> cls = obj.getClass();
            /*
            if (cls.isArray()) {
                // Special case for byte[] -> String
                if (obj instanceof byte[]) {
                    byte[] b = (byte[]) obj;
                    buffer.putByte(VoltType.STRING.getValue());
                    buffer.putShort(b.length);
                    buffer.put(b);
                    continue;
                }

                buffer.writeByte(ARRAY);
                VoltType type = VoltType.typeFromClass(cls.getComponentType());
                buffer.writeByte(type.getValue());
                switch (type) {
                case INTEGER:
                    buffer.putArray((long[]) obj); break;
                case FLOAT:
                    buffer.putArray((double[]) obj); break;
                case STRING:
                    buffer.putArray((String[]) obj); break;
                case TIMESTAMP:
                    buffer.putArray((Date[]) obj); break;
                default:
                    throw new RuntimeException("FIXME: Unsupported type " + type);
                }
                continue;
            }
            */

            VoltType type = VoltType.typeFromClass(cls);
            buffer.put(type.getValue());
            switch (type) {
            case INTEGER:
                buffer.putLong((Long) obj); break;
            case FLOAT:
                buffer.putDouble((Double) obj); break;
            case STRING:
                writeString(buffer, ((String) obj)); break;
            case TIMESTAMP:
                buffer.putLong(((Date) obj).getTime()); break;
            }
        }
    }
    private static void writeString(ByteBuffer buffer, String string) {
        final short NULL_STRING_INDICATOR = -1;
        if (string == null) {
            buffer.putShort(NULL_STRING_INDICATOR);
            return;
        }
        
        // assume not null now
        int len = string.length();
        buffer.putShort((short) len);
        byte[] strbytes = {};
        try {
            // this will need to be less "western" in the future
            strbytes = string.getBytes("ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        assert len == strbytes.length;
        buffer.put(strbytes);
    }
    public enum VoltType {
        INTEGER     ((byte)6),
        FLOAT       ((byte)8),
        TIMESTAMP   ((byte)11),
        STRING      ((byte)9),
        MONEY       ((byte)20),
        VOLTTABLE   ((byte)21),
        INVALID     ((byte)0),
        NULL        ((byte)1);
        
        private final byte val;
        
        VoltType(byte val) {
            this.val = val;
        }   
        public byte getValue() {
            return val;
        }
        public static VoltType get(byte val) {
            for (VoltType type : VoltType.values()) {
                if (type.val == val) return type;
            }
            throw new AssertionError("Unknown type: " + String.valueOf(val));
        }
        
        public static VoltType typeFromClass(Class<?> cls) {
            if ((cls == long.class) || 
                (cls == Long.class) || 
                (cls == Integer.class) ||
                (cls == int.class))
                return INTEGER;
            if ((cls == Float.class) || 
                (cls == float.class) ||
                (cls == Double.class) ||
                (cls == double.class))
                return FLOAT;
            if ((cls == Date.class))
                return TIMESTAMP;
            if ((cls == String.class))
                return STRING;
            
            throw new RuntimeException("Unimplemented Object Type");
        }
    }
    private static class MyLEHeapBuffer {
        byte[] bytes;
        int pos = 0;
        MyLEHeapBuffer(byte[] bytes) { this.bytes = bytes; }
        int position() { return pos;}
        void clear() {pos = 0;}
        void put(byte[] b) {
            System.arraycopy(b, 0, bytes, pos, b.length);
            pos += b.length;
        }
        void putByte(byte b) {bytes[pos++] = b;}
        void putString(String string) {
            final short NULL_STRING_INDICATOR = -1;
            if (string == null) {
                putShort(NULL_STRING_INDICATOR);
                return;
            }
            
            // assume not null now
            int len = string.length();
            putShort((short) len);
            byte[] strbytes = {};
            try {
                // this will need to be less "western" in the future
                strbytes = string.getBytes("ISO-8859-1");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
            assert len == strbytes.length;
            put(strbytes);
        }
        void putShort(short v) {
            bytes[pos++] = (byte) (v >> 0);
            bytes[pos++] = (byte) (v >> 8);
        }
        void putLong(long v) {
            bytes[pos++] = (byte) (v >> 0);
            bytes[pos++] = (byte) (v >> 8);
            bytes[pos++] = (byte) (v >> 16);
            bytes[pos++] = (byte) (v >> 24);
            bytes[pos++] = (byte) (v >> 32);
            bytes[pos++] = (byte) (v >> 40);
            bytes[pos++] = (byte) (v >> 48);
            bytes[pos++] = (byte) (v >> 56);
            /* which is faster?
            bytes[pos] = (byte) (v >> 0);
            bytes[pos+1] = (byte) (v >> 8);
            bytes[pos+2] = (byte) (v >> 16);
            bytes[pos+3] = (byte) (v >> 24);
            bytes[pos+4] = (byte) (v >> 32);
            bytes[pos+5] = (byte) (v >> 40);
            bytes[pos+6] = (byte) (v >> 48);
            bytes[pos+7] = (byte) (v >> 56);
            pos += 8;
            */
        }
        void putDouble(double v) {
            putLong(Double.doubleToRawLongBits(v));
        }
    }
    private static void writeParams(Object[] params, MyLEHeapBuffer buffer) {
        buffer.putByte((byte) params.length);
        
        for (Object obj : params) {
            if (obj == null) {
                VoltType type = VoltType.NULL;
                buffer.putByte(type.getValue());
                continue;
            }

            Class<?> cls = obj.getClass();

            VoltType type = VoltType.typeFromClass(cls);
            buffer.putByte(type.getValue());
            switch (type) {
            case INTEGER:
                buffer.putLong((Long) obj); break;
            case FLOAT:
                buffer.putDouble((Double) obj); break;
            case STRING:
                buffer.putString((String) obj); break;
            case TIMESTAMP:
                buffer.putLong(((Date) obj).getTime()); break;
            }
        }
    }
}

