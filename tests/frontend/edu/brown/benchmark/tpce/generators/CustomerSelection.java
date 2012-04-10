/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Alex Kalinin (akalinin@cs.brown.edu)                                   *
 *  http://www.cs.brown.edu/~akalinin/                                     *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/

package edu.brown.benchmark.tpce.generators;


/******************************************************************************
*   Description:        This class encapsulates customer tier distribution
*                       functions and provides functionality to:
*                       - Generate customer tier based on customer ID
*                       - Generate non-uniform customer ID
*                       - Generate customer IDs in a specified partition, and
*                         outside the specified partition a set percentage of
*                         the time.
******************************************************************************/

public class CustomerSelection {


    // lower 3 difgits
    private static long lowDigits(long cid) {
        return (cid - 1) % 1000;
    }

    // higher 3 digits
    private static long highDigits(long cid) {
        return (cid - 1) / 1000;
    }
    
    public static long permute(long low, long high) {
        return  (677 * low + 33 * (high + 1)) % 1000;
    }

    // Inverse permutation.
    public static long inversePermute(long low, long high) {
        // Extra mod to make the result always positive
        return  (((613 * (low - 33 * (high + 1))) % 1000) + 1000) % 1000;
    }
    
    public static short getTier(long cid) {
        long revCid = inversePermute(lowDigits(cid), highDigits(cid));

        if (revCid < 200) {
            return 1;
        }
        else {
            if (revCid < 800) {
                return 2;
            }
            else {
                return 3;
            }
        }
    }
}
