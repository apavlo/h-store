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

package edu.brown.benchmark.tpce.util;

/**
 * Small class for manipulating money
 */
public class EGenMoney {
    private int amountInCents;

    public EGenMoney(double dollars) {
        amountInCents = (int)(100.0 * dollars + 0.5);
    }
    
    public EGenMoney(EGenMoney money) {
        this.amountInCents = money.amountInCents;
    }
    
    public double getDollars() {
        return (double)amountInCents / 100;
    }
    
    public void multiplyByDouble(double mul) {
        /*
         *  Do a trick for correct rounding. Can't use ceil or floor functions
         *  because they do not round properly (e.g. down when < 0.5, up when >= 0.5).
         */
        if (amountInCents > 0) {
            amountInCents = (int)(amountInCents * mul + 0.5);
        }
        else {
            amountInCents = (int)(amountInCents * mul - 0.5);
        }
    }
    
    public void multiplyByInt(int mul) {
        amountInCents *= mul;
    }
    
    public void add(EGenMoney money) {
        amountInCents += money.amountInCents;
    }
    
    public void mul(EGenMoney money) {
        amountInCents *= money.amountInCents;
    }
    
    public void sub(EGenMoney money) {
        amountInCents -= money.amountInCents;
    }
    
    public boolean lessThanOrEqual(EGenMoney m) {
        return amountInCents <= m.amountInCents;
    }
    
    public boolean lessThan(EGenMoney m) {
        return amountInCents < m.amountInCents;
    }
    
    public static EGenMoney subMoney(EGenMoney m1, EGenMoney m2) {
        EGenMoney res = new EGenMoney(m1);
        res.sub(m2);
        
        return res;
    }
    
    public static EGenMoney addMoney(EGenMoney m1, EGenMoney m2) {
        EGenMoney res = new EGenMoney(m1);
        res.add(m2);
        
        return res;
    }
    
    public static EGenMoney mulMoney(EGenMoney m1, EGenMoney m2) {
        EGenMoney res = new EGenMoney(m1);
        res.mul(m2);
        
        return res;
    }
    
    public static EGenMoney mulMoneyByInt(EGenMoney m1, int m2) {
        EGenMoney res = new EGenMoney(m1);
        res.amountInCents *= m2;
        
        return res;
    }
}
