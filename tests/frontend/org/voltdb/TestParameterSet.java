/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.voltdb.ParameterSet;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.types.TimestampType;

import junit.framework.TestCase;

public class TestParameterSet extends TestCase {
    ParameterSet params;

    @Override
    public void setUp() {
        params = new ParameterSet();
    }

    public void testNull() throws IOException {
        params.setParameters(new Object[]{null, null, null});
        ByteBuffer buf = ByteBuffer.wrap(FastSerializer.serialize(params));
        buf.rewind();

        ParameterSet out = new ParameterSet();
        out.readExternal(new FastDeserializer(buf));

        assertEquals(3, out.toArray().length);
        assertNull(out.toArray()[0]);
    }

    public void testStrings() throws IOException {
        params.setParameters(new Object[]{"foo"});
        ByteBuffer buf = ByteBuffer.wrap(FastSerializer.serialize(params));
        buf.rewind();

        ParameterSet out = new ParameterSet();
        out.readExternal(new FastDeserializer(buf));
        assertEquals(1, out.toArray().length);
        assertEquals("foo", out.toArray()[0]);
    }

    public void testStringsAsByteArray() throws IOException {
        params = new ParameterSet();
        params.setParameters(new Object[]{new byte[]{'f', 'o', 'o'}});
        ByteBuffer buf = ByteBuffer.wrap(FastSerializer.serialize(params));
        buf.rewind();

        ParameterSet out = new ParameterSet();
        out.readExternal(new FastDeserializer(buf));
        assertEquals(1, out.toArray().length);

        byte[] bin = (byte[]) out.toArray()[0];
        assertEquals(bin[0], 'f'); assertEquals(bin[1], 'o'); assertEquals(bin[2], 'o');
    }

    private boolean arrayLengthTester(Object[] objs)
    {
        params = new ParameterSet();
        params.setParameters(objs);
        boolean threw = false;
        try
        {
            ByteBuffer buf = ByteBuffer.wrap(FastSerializer.serialize(params));
        }
        catch (IOException ioe)
        {
            threw = true;
        }
        return threw;
    }

    public void testArraysTooLong() throws IOException {
        assertTrue("Array longer than Short.MAX_VALUE didn't fail to serialize",
                   arrayLengthTester(new Object[]{new short[Short.MAX_VALUE + 1]}));
        assertTrue("Array longer than Short.MAX_VALUE didn't fail to serialize",
                   arrayLengthTester(new Object[]{new int[Short.MAX_VALUE + 1]}));
        assertTrue("Array longer than Short.MAX_VALUE didn't fail to serialize",
                   arrayLengthTester(new Object[]{new long[Short.MAX_VALUE + 1]}));
        assertTrue("Array longer than Short.MAX_VALUE didn't fail to serialize",
                   arrayLengthTester(new Object[]{new double[Short.MAX_VALUE + 1]}));
        assertTrue("Array longer than Short.MAX_VALUE didn't fail to serialize",
                   arrayLengthTester(new Object[]{new String[Short.MAX_VALUE + 1]}));
        assertTrue("Array longer than Short.MAX_VALUE didn't fail to serialize",
                   arrayLengthTester(new Object[]{new TimestampType[Short.MAX_VALUE + 1]}));
        assertTrue("Array longer than Short.MAX_VALUE didn't fail to serialize",
                   arrayLengthTester(new Object[]{new BigDecimal[Short.MAX_VALUE + 1]}));
    }
}
