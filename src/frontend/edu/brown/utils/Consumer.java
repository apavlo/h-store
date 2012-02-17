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
package edu.brown.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Consumer<T> implements Runnable {

    private final LinkedBlockingDeque<T> queue = new LinkedBlockingDeque<T>();
    private Semaphore start = new Semaphore(0);
    private final AtomicBoolean stopWhenEmpty = new AtomicBoolean(false);
    private Thread self;
    private int counter = 0;

    public Consumer() {
        // Nothing...
    }

    @Override
    public void run() {
        this.self = Thread.currentThread();
        this.counter = 0;

        // Wait until we have our producer
        this.start.acquireUninterruptibly();

        T t = null;
        while (true) {
            try {
                // If the producer has queued everything, poll it right away
                // That way we'll know when our queue is empty for good
                if (this.stopWhenEmpty.get()) {
                    t = this.queue.poll();
                }
                // Otherwise block until something gets added
                else {
                    t = this.queue.take();
                }

                // If the next item is null, then we want to stop right away
                if (t == null)
                    break;

                this.process(t);
                this.counter++;
            } catch (InterruptedException ex) {
                // Ignore
            }
        } // WHILE
    }

    public abstract void process(T t);

    public int getProcessedCounter() {
        return (this.counter);
    }

    public void queue(T t) {
        this.queue.add(t);
    }

    public synchronized final void reset() {
        this.self = null;
        this.queue.clear();
        this.stopWhenEmpty.set(false);
        this.start.drainPermits();
    }

    public final void start() {
        this.start.release();
    }

    public final void stopWhenEmpty() {
        this.stopWhenEmpty.set(true);
        if (this.self != null) {
            this.self.interrupt();
        }
    }
}
