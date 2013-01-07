/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
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
package edu.brown.profilers;

/**
 * Special thread-safe ProfileMeasurement
 * @author pavlo
 */
public class ConcurrentProfileMeasurement extends ProfileMeasurement {

    public ConcurrentProfileMeasurement() {
        super();
    }
    public ConcurrentProfileMeasurement(String pmtype) {
        super(pmtype);
    }
    public ConcurrentProfileMeasurement(ProfileMeasurement orig) {
        super(orig);
    }
    
    @Override
    public synchronized void reset() {
        super.reset();
    }
    @Override
    public synchronized void clear() {
        super.clear();
    }
    @Override
    public synchronized ProfileMeasurement start(long timestamp) {
        return super.start(timestamp);
    }
    @Override
    public synchronized ProfileMeasurement stop(long timestamp) {
        return super.stop(timestamp);
    }
    @Override
    public synchronized ProfileMeasurement stopIfStarted(long timestamp) {
        return super.stopIfStarted(timestamp);
    }
    @Override
    public synchronized ProfileMeasurement decrementTime(ProfileMeasurement other, boolean checkType) {
        return super.decrementTime(other, checkType);
    }
    public synchronized ProfileMeasurement appendTime(ProfileMeasurement other) {
        return super.appendTime(other);
    }
    @Override
    public synchronized void appendTime(long start, long stop, int invocations) {
        super.appendTime(start, stop, invocations);
    }
}